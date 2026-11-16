//! MongoDB's driver for SpelaionDB: MongoDB and the servers that speak its protocol (FerretDB),
//! served as a plugin (SpelaionDB's ADR-041, ADR-056).
//!
//! It is sent **requests**, never statements (protocol 1.1): SpelaionDB's core reads what the
//! user typed and turns each statement into one command document, which this driver runs as it
//! is and never changes or reads as text. Results are documents laid out as rows and columns
//! (`results.rs`).
//!
//! The core opens every route to the server (tunnels, proxies, Kubernetes): this driver connects
//! only to the address it is given, directly, and never discovers other members of a replica
//! set or looks servers up in DNS (`mongodb+srv`).

mod results;
mod route;

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use mongodb::bson::{Bson, Document, doc};
use mongodb::error::{Error as MongoError, ErrorKind};
use mongodb::options::{AuthMechanism, ClientOptions, Credential, ServerAddress, Tls, TlsOptions};
use mongodb::{Client, Cursor};
use serde_json::Value;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, IndexInfo, KeepAlive, ObjectInfo, ObjectKind, Result, SchemaColumn,
    SchemaInfo, TableInfo, TlsMode,
};
use tokio::sync::{Notify, mpsc};

pub use results::{LEFTOVERS, Layout, MAX_FIELDS};

/// The settings a connection may give (`connect`'s `options`): checked again here, after the
/// core, so a setting nobody looked at never changes how the plugin connects.
pub const OPTIONS: &[&str] = &["authSource", "authMechanism", "replicaSet"];

/// The commands whose answer is a cursor, read a batch at a time as the core asks.
const CURSOR_COMMANDS: &[&str] = &["find", "aggregate", "listCollections", "listIndexes"];

/// The commands whose answer's `n` is how many documents they changed.
const WRITE_COMMANDS: &[&str] = &["insert", "update", "delete"];

/// The databases MongoDB keeps for itself.
const SYSTEM_DATABASES: &[&str] = &["admin", "config", "local"];

/// The `mongodb` engine.
#[derive(Debug, Default, Clone)]
pub struct MongoDriver;

#[async_trait]
impl Driver for MongoDriver {
    /// The name SpelaionDB's configuration uses: `engine = "mongodb"`.
    fn engine(&self) -> &str {
        "mongodb"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if params.read_only {
            // SpelaionDB's core does not open such a connection (ADR-056, point 6); if it ever
            // did, nothing here could keep it read-only.
            return Err(DriverError::Connection(
                "MongoDB has no read-only mode in SpelaionDB yet".into(),
            ));
        }
        let mut options = client_options(params)?;
        let route = match params.connect_addr {
            Some(address) => {
                let route = route::Route::open(params.host.clone(), params.port, address)
                    .await
                    .map_err(|_| {
                        DriverError::Connection("could not open the local MongoDB route".into())
                    })?;
                options.socks5_proxy = Some(route.proxy());
                Some(route)
            }
            None => None,
        };
        let client = Client::with_options(options).map_err(connection_error)?;
        let database = if params.database.is_empty() {
            "test".to_string()
        } else {
            params.database.clone()
        };
        // The driver connects lazily: a round trip says now whether the login works.
        let timeout = params.connect_timeout();
        match tokio::time::timeout(
            timeout,
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(connection_error(error)),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    timeout.as_secs()
                )));
            }
        }
        Ok(Box::new(MongoConnection {
            client,
            database: Mutex::new(database),
            capabilities: Capabilities {
                cancel: true,
                schemas: true,
                ..Capabilities::default()
            },
            running: Arc::new(Mutex::new(None)),
            _route: route,
        }))
    }
}

/// How the driver reaches the server, from what `connect` said.
fn client_options(params: &ConnectParams) -> Result<ClientOptions> {
    for key in params.options.keys() {
        if !OPTIONS.contains(&key.as_str()) {
            return Err(DriverError::Connection(format!(
                "MongoDB has no connection setting `{key}` here (it knows {})",
                OPTIONS.join(", ")
            )));
        }
    }
    let tls = match params.tls {
        // MongoDB has no "encrypt if the server can": `prefer` connects without TLS.
        TlsMode::Disable | TlsMode::Prefer => None,
        TlsMode::Require => Some(
            TlsOptions::builder()
                .allow_invalid_certificates(true)
                .build(),
        ),
        // The SOCKS transport keeps this logical name for TLS while reaching only the
        // address supplied by the core. Verify-ca also checks the name (stricter).
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let cert_key = match (&params.client_certificate, &params.client_key) {
                (None, None) => None,
                (Some(cert), Some(key)) if cert == key => Some(cert.clone()),
                _ => {
                    return Err(DriverError::Connection(
                        "MongoDB reads a client certificate and its key from one PEM file: give \
                         the same file as both"
                            .into(),
                    ));
                }
            };
            Some(
                TlsOptions::builder()
                    .ca_file_path(params.ca_certificate.clone())
                    .cert_key_file_path(cert_key)
                    .build(),
            )
        }
    };
    let credential = if params.user.is_empty() {
        None
    } else {
        let source = params
            .options
            .get("authSource")
            .cloned()
            .unwrap_or_else(|| "admin".to_string());
        let mechanism = match params.options.get("authMechanism") {
            None => None,
            Some(name) => match AuthMechanism::from_str(name) {
                Ok(m @ (AuthMechanism::ScramSha1 | AuthMechanism::ScramSha256)) => Some(m),
                _ => {
                    return Err(DriverError::Connection(format!(
                        "authMechanism `{name}` is not one SpelaionDB logs in with: \
                         SCRAM-SHA-1 or SCRAM-SHA-256"
                    )));
                }
            },
        };
        Some(
            Credential::builder()
                .username(params.user.clone())
                .password(params.password.as_ref().map(|p| p.expose().to_string()))
                .source(source)
                .mechanism(mechanism)
                .build(),
        )
    };
    let timeout = params.connect_timeout();
    Ok(ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: params.host.clone(),
            port: Some(params.port),
        }])
        // Only the address given: never the other members a server names.
        .direct_connection(true)
        // DNS TXT options have already been checked by the core. Check the expected set
        // without discovering or connecting to any other member, including through a tunnel.
        .repl_set_name(params.options.get("replicaSet").cloned())
        .app_name(params.application_name.clone())
        .credential(credential)
        .tls(tls.map(Tls::Enabled))
        .connect_timeout(timeout)
        .server_selection_timeout(timeout)
        // Nothing the user ran is ever run again (SpelaionDB's AGENTS.md).
        .retry_reads(false)
        .retry_writes(false)
        .build())
}

struct MongoConnection {
    client: Client,
    database: Mutex<String>,
    capabilities: Capabilities,
    /// Told when the request that is running must stop.
    running: Arc<Mutex<Option<Arc<Notify>>>>,
    /// Dropping a session also closes its listener and all its forwarded sockets.
    _route: Option<route::Route>,
}

/// A request, checked: the database it runs in and its command.
struct Request {
    database: String,
    command: Document,
}

impl Request {
    fn from_value(value: Value) -> Result<Request> {
        let Value::Object(mut fields) = value else {
            return Err(bad_request("it is not an object"));
        };
        let database = match fields.remove("database") {
            Some(Value::String(database)) if !database.is_empty() => database,
            _ => return Err(bad_request("it names no database")),
        };
        let command = match fields.remove("command") {
            Some(command @ Value::Object(_)) => command,
            _ => return Err(bad_request("it has no command document")),
        };
        if let Some(other) = fields.keys().next() {
            return Err(bad_request(&format!("it has a field `{other}`")));
        }
        let command = match Bson::try_from(command) {
            Ok(Bson::Document(command)) if !command.is_empty() => command,
            Ok(_) => return Err(bad_request("its command is empty")),
            Err(error) => {
                return Err(bad_request(&format!(
                    "its command is not Extended JSON: {error}"
                )));
            }
        };
        Ok(Request { database, command })
    }

    /// The command's name: its first key.
    fn name(&self) -> &str {
        self.command.keys().next().map(String::as_str).unwrap_or("")
    }
}

fn bad_request(why: &str) -> DriverError {
    DriverError::Other(format!(
        "the request SpelaionDB sent is not a MongoDB command: {why}"
    ))
}

#[async_trait]
impl Connection for MongoConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, _sql: &str, _opts: &ExecOptions) -> Result<EventStream> {
        Err(DriverError::Other(
            "MongoDB runs requests, not statements: SpelaionDB's core sends each one as a \
             command document"
                .into(),
        ))
    }

    async fn execute_request(&self, request: Value, opts: &ExecOptions) -> Result<EventStream> {
        let request = Request::from_value(request)?;
        let cancelled = Arc::new(Notify::new());
        *self.running.lock().expect("lock poisoned") = Some(cancelled.clone());
        let database = self.client.database(&request.database);
        let batch_size = opts.batch_size.max(1);
        // One event at a time: the core's credit decides when the next is read.
        let (sender, receiver) = mpsc::channel::<Result<ExecEvent>>(1);
        tokio::spawn(async move {
            let run = async {
                if CURSOR_COMMANDS.contains(&request.name()) {
                    let cursor = database
                        .run_cursor_command(request.command)
                        .await
                        .map_err(database_error)?;
                    stream_cursor(cursor, batch_size, &sender).await
                } else {
                    let name = request.name().to_string();
                    let reply = database
                        .run_command(request.command)
                        .await
                        .map_err(database_error)?;
                    let rows_affected = if WRITE_COMMANDS.contains(&name.as_str()) {
                        number(reply.get("n"))
                    } else {
                        None
                    };
                    let layout = Layout::from_first(std::slice::from_ref(&reply));
                    let batch = layout.batch(std::slice::from_ref(&reply))?;
                    send(&sender, Ok(ExecEvent::Columns(layout.schema()))).await?;
                    send(&sender, Ok(ExecEvent::Batch(batch))).await?;
                    Ok(rows_affected)
                }
            };
            let outcome = tokio::select! {
                biased;
                () = cancelled.notified() => Err(Stopped::Failed(DriverError::Cancelled)),
                outcome = run => outcome,
            };
            let last = match outcome {
                Ok(rows_affected) => Ok(ExecEvent::Done { rows_affected }),
                // The core stopped reading: nobody is there to tell.
                Err(Stopped::Gone) => return,
                Err(Stopped::Failed(error)) => Err(error),
            };
            let _ = sender.send(last).await;
        });
        Ok(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(Cancel(self.running.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let info = self
            .client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(database_error)?;
        Ok(info.get_str("version").unwrap_or("").to_string())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        self.client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(connection_error)?;
        Ok(KeepAlive::Alive)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(Some(self.database.lock().expect("lock poisoned").clone()))
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.current_database().await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        *self.database.lock().expect("lock poisoned") = database.to_string();
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        self.client
            .list_database_names()
            .await
            .map_err(database_error)
    }

    /// A MongoDB database is what the explorer shows as a schema, as MySQL's are.
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(self
            .list_databases()
            .await?
            .into_iter()
            .map(|name| SchemaInfo {
                system: SYSTEM_DATABASES.contains(&name.as_str()),
                name,
            })
            .collect())
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let mut cursor = self
            .client
            .database(schema)
            .run_cursor_command(doc! { "listCollections": 1, "authorizedCollections": true })
            .await
            .map_err(database_error)?;
        let mut objects = Vec::new();
        while let Some(entry) = cursor.next().await {
            let entry = entry.map_err(database_error)?;
            let Ok(name) = entry.get_str("name") else {
                continue;
            };
            let kind = entry.get_str("type").unwrap_or("collection");
            objects.push(ObjectInfo {
                schema: schema.to_string(),
                name: name.to_string(),
                kind: ObjectKind::Collection,
                // A count would read every collection: nothing the app runs on its own may.
                estimated_rows: None,
                comment: (kind != "collection").then(|| kind.to_string()),
            });
        }
        objects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(objects)
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let object = self
            .list_objects(schema)
            .await?
            .into_iter()
            .find(|o| o.name == name)
            .ok_or_else(|| DriverError::Other(format!("there is no collection {schema}.{name}")))?;
        let mut indexes = Vec::new();
        // A view has no indexes, and its server says so with an error.
        if object.comment.is_none() {
            let mut cursor = self
                .client
                .database(schema)
                .run_cursor_command(doc! { "listIndexes": name })
                .await
                .map_err(database_error)?;
            while let Some(index) = cursor.next().await {
                let index = index.map_err(database_error)?;
                let index_name = index.get_str("name").unwrap_or("").to_string();
                indexes.push(IndexInfo {
                    primary: index_name == "_id_",
                    unique: index.get_bool("unique").unwrap_or(false),
                    definition: results::json(&Bson::Document(index)),
                    name: index_name,
                });
            }
        }
        Ok(TableInfo {
            object,
            // A collection has no fixed fields.
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexes,
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    async fn list_columns(&self, _schema: &str) -> Result<Vec<SchemaColumn>> {
        // A collection has no fixed fields, and reading a sample of each would read data.
        Ok(Vec::new())
    }
}

/// Why sending events stopped.
enum Stopped {
    /// The core dropped the stream.
    Gone,
    Failed(DriverError),
}

impl From<DriverError> for Stopped {
    fn from(error: DriverError) -> Self {
        Stopped::Failed(error)
    }
}

async fn send(
    sender: &mpsc::Sender<Result<ExecEvent>>,
    event: Result<ExecEvent>,
) -> std::result::Result<(), Stopped> {
    sender.send(event).await.map_err(|_| Stopped::Gone)
}

/// A cursor's documents, `batch_size` at a time: the first batch decides the columns.
async fn stream_cursor(
    mut cursor: Cursor<Document>,
    batch_size: usize,
    sender: &mpsc::Sender<Result<ExecEvent>>,
) -> std::result::Result<Option<u64>, Stopped> {
    let mut layout: Option<Layout> = None;
    loop {
        let mut documents = Vec::with_capacity(batch_size.min(10_000));
        while documents.len() < batch_size {
            match cursor.next().await {
                Some(document) => documents.push(document.map_err(database_error)?),
                None => break,
            }
        }
        let last = documents.len() < batch_size;
        let layout = match &layout {
            Some(layout) => layout,
            None => {
                let first = Layout::from_first(&documents);
                send(sender, Ok(ExecEvent::Columns(first.schema()))).await?;
                layout.insert(first)
            }
        };
        if !documents.is_empty() {
            send(sender, Ok(ExecEvent::Batch(layout.batch(&documents)?))).await?;
        }
        if last {
            return Ok(None);
        }
    }
}

fn number(value: Option<&Bson>) -> Option<u64> {
    match value? {
        Bson::Int32(n) => u64::try_from(*n).ok(),
        Bson::Int64(n) => u64::try_from(*n).ok(),
        Bson::Double(n) if *n >= 0.0 && n.fract() == 0.0 => Some(*n as u64),
        _ => None,
    }
}

/// An error of a command the server refused, with its code; anything else as it is.
fn database_error(error: MongoError) -> DriverError {
    match error.kind.as_ref() {
        ErrorKind::Command(command) => DriverError::Database {
            code: Some(if command.code_name.is_empty() {
                command.code.to_string()
            } else {
                command.code_name.clone()
            }),
            message: command.message.clone(),
            detail: None,
            hint: None,
            position: None,
        },
        ErrorKind::Write(_) | ErrorKind::InsertMany(_) | ErrorKind::BulkWrite(_) => {
            DriverError::Database {
                code: None,
                message: error.to_string(),
                detail: None,
                hint: None,
                position: None,
            }
        }
        _ => connection_error(error),
    }
}

/// An error reaching or logging in to the server.
fn connection_error(error: MongoError) -> DriverError {
    match error.kind.as_ref() {
        ErrorKind::Authentication { message, .. } => {
            DriverError::Connection(format!("the server refused the login: {message}"))
        }
        ErrorKind::Command(_) => database_error(error),
        _ => DriverError::Connection(error.to_string()),
    }
}

struct Cancel(Arc<Mutex<Option<Arc<Notify>>>>);

#[async_trait]
impl CancelHandle for Cancel {
    async fn cancel(&self) -> Result<()> {
        // `notify_one` keeps the wake-up if the request has not started waiting yet. Dropping
        // the request closes its cursor on the server.
        if let Some(running) = self.0.lock().expect("lock poisoned").as_ref() {
            running.notify_one();
        }
        Ok(())
    }
}

#[cfg(test)]
mod connection_options_tests {
    use super::*;

    #[test]
    fn expected_replica_set_does_not_enable_discovery_or_retries() {
        let mut params = ConnectParams {
            host: "node.example.test".into(),
            port: 27123,
            user: "user".into(),
            tls: TlsMode::VerifyFull,
            ..Default::default()
        };
        params.options.insert("replicaSet".into(), "atlas-1".into());
        params.options.insert("authSource".into(), "admin".into());
        let options = client_options(&params).unwrap();
        assert_eq!(options.repl_set_name.as_deref(), Some("atlas-1"));
        assert_eq!(options.direct_connection, Some(true));
        assert_eq!(options.retry_reads, Some(false));
        assert_eq!(options.retry_writes, Some(false));
        assert_eq!(
            options.hosts,
            vec![ServerAddress::Tcp {
                host: "node.example.test".into(),
                port: Some(27123),
            }]
        );
        assert_eq!(options.credential.unwrap().source.as_deref(), Some("admin"));
        let Some(Tls::Enabled(tls)) = options.tls else {
            panic!("TLS required")
        };
        assert_ne!(tls.allow_invalid_certificates, Some(true));
    }

    #[test]
    fn expected_set_keeps_the_cores_route_and_verified_tls_is_never_weakened() {
        let mut params = ConnectParams {
            host: "node.example.test".into(),
            port: 27123,
            connect_addr: Some("127.0.0.1:12345".parse().unwrap()),
            ..Default::default()
        };
        params.options.insert("replicaSet".into(), "atlas-1".into());
        let options = client_options(&params).unwrap();
        assert_eq!(options.repl_set_name.as_deref(), Some("atlas-1"));
        assert_eq!(
            options.hosts,
            vec![ServerAddress::Tcp {
                host: "node.example.test".into(),
                port: Some(27123),
            }]
        );
        params.tls = TlsMode::VerifyFull;
        let options = client_options(&params).unwrap();
        let Some(Tls::Enabled(tls)) = options.tls else {
            panic!("TLS required")
        };
        assert_ne!(tls.allow_invalid_certificates, Some(true));
    }
}
