//! A fixed socket transport for MongoDB's supported SOCKS5 connector. MongoDB retains the
//! logical hostname for TLS; this adapter never resolves or connects a requested destination.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use mongodb::options::Socks5Proxy;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SOCKETS: usize = 128;

pub(crate) struct Route {
    proxy: Socks5Proxy,
    task: JoinHandle<()>,
}

impl Drop for Route {
    fn drop(&mut self) {
        // The task owns its JoinSet: aborting it drops and aborts every forwarded socket.
        self.task.abort();
    }
}

impl Route {
    pub(crate) async fn open(host: String, port: u16, address: SocketAddr) -> io::Result<Self> {
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(io::Error::other)?;
        let password: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let local = listener.local_addr()?;
        let proxy = Socks5Proxy::builder()
            .host(local.ip().to_string())
            .port(local.port())
            .authentication(("spelaion".into(), password.clone()))
            .build();
        let destination = Arc::new(Destination {
            host,
            port,
            address,
            password,
        });
        let task = tokio::spawn(async move {
            let mut sockets = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, peer)) = accepted else { break };
                        if !peer.ip().is_loopback() || sockets.len() >= MAX_SOCKETS {
                            continue;
                        }
                        let destination = destination.clone();
                        sockets.spawn(async move {
                            let _ = destination.forward(socket).await;
                        });
                    }
                    _ = sockets.join_next(), if !sockets.is_empty() => {}
                }
            }
        });
        Ok(Self { proxy, task })
    }

    pub(crate) fn proxy(&self) -> Socks5Proxy {
        self.proxy.clone()
    }
}

struct Destination {
    host: String,
    port: u16,
    address: SocketAddr,
    password: String,
}

fn refused() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "invalid local MongoDB route request",
    )
}

async fn bytes(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = socket.read_u8().await?;
    let mut value = vec![0; usize::from(length)];
    socket.read_exact(&mut value).await?;
    Ok(value)
}

impl Destination {
    async fn authenticate(&self, socket: &mut TcpStream) -> io::Result<()> {
        if socket.read_u8().await? != 5 || !bytes(socket).await?.contains(&2) {
            return Err(refused());
        }
        socket.write_all(&[5, 2]).await?;
        if socket.read_u8().await? != 1
            || bytes(socket).await? != b"spelaion"
            || !bool::from(bytes(socket).await?.ct_eq(self.password.as_bytes()))
        {
            return Err(refused());
        }
        socket.write_all(&[1, 0]).await?;
        let mut header = [0; 4];
        socket.read_exact(&mut header).await?;
        // Only CONNECT: never BIND or UDP, and no destination chosen by the caller.
        if header[..3] != [5, 1, 0] {
            return Err(refused());
        }
        let matches = match header[3] {
            1 => {
                let mut octets = [0; 4];
                socket.read_exact(&mut octets).await?;
                self.host.parse::<IpAddr>().ok() == Some(Ipv4Addr::from(octets).into())
            }
            4 => {
                let mut octets = [0; 16];
                socket.read_exact(&mut octets).await?;
                self.host.parse::<IpAddr>().ok() == Some(Ipv6Addr::from(octets).into())
            }
            3 => bytes(socket).await? == self.host.as_bytes(),
            _ => false,
        };
        if !matches || socket.read_u16().await? != self.port {
            return Err(refused());
        }
        Ok(())
    }

    async fn forward(&self, mut socket: TcpStream) -> io::Result<()> {
        let mut remote = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
            self.authenticate(&mut socket).await?;
            // Crucially, this is the core's fixed socket, never a parsed SOCKS address.
            TcpStream::connect(self.address).await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "local MongoDB route timed out"))??;
        socket.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
        tokio::io::copy_bidirectional(&mut socket, &mut remote).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn request(
        route: &Route,
        password: &str,
        host: &str,
        port: u16,
        command: u8,
    ) -> TcpStream {
        let proxy = route.proxy();
        let mut socket = TcpStream::connect((proxy.host.as_str(), proxy.port.unwrap()))
            .await
            .unwrap();
        socket.write_all(&[5, 1, 2]).await.unwrap();
        let mut reply = [0; 2];
        socket.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [5, 2]);
        let mut auth = vec![1, 8];
        auth.extend_from_slice(b"spelaion");
        auth.push(password.len() as u8);
        auth.extend_from_slice(password.as_bytes());
        socket.write_all(&auth).await.unwrap();
        if socket.read_exact(&mut reply).await.is_ok() {
            assert_eq!(reply, [1, 0]);
            let mut request = vec![5, command, 0, 3, host.len() as u8];
            request.extend_from_slice(host.as_bytes());
            request.extend_from_slice(&port.to_be_bytes());
            socket.write_all(&request).await.unwrap();
        }
        socket
    }

    #[tokio::test]
    async fn handshakes_are_bounded_and_socket_limit_cannot_open_an_extra_route() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let remote = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let route = Route::open(
                "mongo.example.test".into(),
                27017,
                remote.local_addr().unwrap(),
            )
            .await
            .unwrap();
            let proxy = route.proxy();
            let mut sockets = Vec::new();
            for _ in 0..MAX_SOCKETS {
                let mut socket = TcpStream::connect((proxy.host.as_str(), proxy.port.unwrap()))
                    .await
                    .unwrap();
                socket.write_all(&[5, 1, 2]).await.unwrap();
                let mut reply = [0; 2];
                socket.read_exact(&mut reply).await.unwrap();
                sockets.push(socket);
            }
            let mut extra = TcpStream::connect((proxy.host.as_str(), proxy.port.unwrap()))
                .await
                .unwrap();
            assert!(extra.read_u8().await.is_err(), "no unbounded socket tasks");
            // Even peers that authenticate no further cannot hold a socket indefinitely.
            assert!(sockets[0].read_u8().await.is_err(), "handshake timeout");
            assert!(
                tokio::time::timeout(Duration::from_millis(20), remote.accept())
                    .await
                    .is_err()
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn only_authenticated_exact_target_connects_and_drop_closes_sockets() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let remote = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let route = Route::open(
                "unresolvable.example.test".into(),
                27017,
                remote.local_addr().unwrap(),
            )
            .await
            .unwrap();
            let password = route.proxy.authentication.as_ref().unwrap().1.clone();
            for (password, host, port, command) in [
                ("wrong", "unresolvable.example.test", 27017, 1),
                (password.as_str(), "other.example.test", 27017, 1),
                (password.as_str(), "unresolvable.example.test", 27018, 1),
                (password.as_str(), "unresolvable.example.test", 27017, 3),
            ] {
                let mut socket = request(&route, password, host, port, command).await;
                assert!(socket.read_u8().await.is_err());
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), remote.accept())
                        .await
                        .is_err()
                );
            }
            let mut sockets = Vec::new();
            for _ in 0..8 {
                let mut local =
                    request(&route, &password, "unresolvable.example.test", 27017, 1).await;
                let (mut server, _) = remote.accept().await.unwrap();
                let mut reply = [0; 10];
                local.read_exact(&mut reply).await.unwrap();
                assert_eq!(reply[1], 0);
                local.write_all(b"client").await.unwrap();
                let mut message = [0; 6];
                server.read_exact(&mut message).await.unwrap();
                assert_eq!(&message, b"client");
                server.write_all(b"server").await.unwrap();
                local.read_exact(&mut message).await.unwrap();
                assert_eq!(&message, b"server");
                sockets.push((local, server));
            }
            let proxy = route.proxy();
            let mut idle = TcpStream::connect((proxy.host.as_str(), proxy.port.unwrap()))
                .await
                .unwrap();
            drop(route);
            for (mut local, mut server) in sockets {
                assert!(local.read_u8().await.is_err());
                assert!(server.read_u8().await.is_err());
            }
            assert!(
                TcpStream::connect((proxy.host.as_str(), proxy.port.unwrap()))
                    .await
                    .is_err()
            );
            assert!(idle.read_u8().await.is_err());
        })
        .await
        .unwrap();
    }
}
