These are public, test-only certificates. `server.pem` contains a key that must never be
used outside this fixture. Its certificate is signed by `ca.crt`, names only
`mongo.example.test`, and expires in November 2036. No CA private key is kept.

The Docker TLS replica set listens on 127.0.0.1:27020. Tests keep the hostname above for
certificate verification and give that socket separately, as the core does for an SSH route.
They also reject a wrong name, an untrusted authority and a wrong replica set.

To exercise the same assertions through a real SSH forward, point
`SPELAION_TEST_MONGO_TLS` at that forward's local socket; the default is the fixture above.
