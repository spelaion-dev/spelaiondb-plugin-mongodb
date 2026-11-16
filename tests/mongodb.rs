//! MongoDB through this plugin's own program, as SpelaionDB's core reaches it: every BSON type
//! as a cell (the type zoo), the layout of documents as rows (SpelaionDB's ADR-056, point 3),
//! the catalog, writes and their counts, errors, and what connecting refuses.
//!
//! It needs the test servers (`ci/services.sh`): `SPELAION_TEST_MONGO=127.0.0.1:27018`, and
//! `SPELAION_TEST_FERRETDB=127.0.0.1:27019` for the tests that also run on FerretDB. Without
//! them it is skipped, which does not count as tested.

use std::time::Duration;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use futures_util::StreamExt;
use mongodb::bson::{Bson, Document, doc, oid::ObjectId};
use serde_json::{Value, json};
use spelaion_driver_api::{
    ColumnMeta, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType, ObjectKind,
    Secret,
};
use spelaion_plugin_host::Plugin;

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-mongodb");

fn server(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(addr) => Some(addr),
        Err(_) => {
            eprintln!("{variable} not set: test skipped");
            None
        }
    }
}

fn params(addr: &str, database: &str) -> spelaion_driver_api::ConnectParams {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    spelaion_driver_api::ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: database.into(),
        connect_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    }
}

/// A client of the tests' own, to put documents where the plugin will read them.
async fn seed(addr: &str, database: &str, collection: &str, documents: Vec<Document>) {
    let client = mongodb::Client::with_uri_str(format!(
        "mongodb://spelaion:spelaion@{addr}/?directConnection=true"
    ))
    .await
    .unwrap();
    let collection = client.database(database).collection::<Document>(collection);
    collection.drop().await.unwrap();
    if !documents.is_empty() {
        collection.insert_many(documents).await.unwrap();
    }
}

async fn connect(plugin: &Plugin, addr: &str, database: &str) -> Box<dyn Connection> {
    tokio::time::timeout(
        Duration::from_secs(30),
        plugin.driver("mongodb").connect(&params(addr, database)),
    )
    .await
    .expect("connecting in time")
    .expect("connecting")
}

/// Everything a request returned, bounded.
async fn run(
    connection: &dyn Connection,
    request: Value,
    batch: usize,
) -> Vec<Result<ExecEvent, DriverError>> {
    let options = ExecOptions {
        batch_size: batch,
        ..Default::default()
    };
    match connection.execute_request(request, &options).await {
        Ok(events) => tokio::time::timeout(Duration::from_secs(60), events.collect())
            .await
            .expect("the request ended in time"),
        Err(error) => vec![Err(error)],
    }
}

struct Outcome {
    columns: Vec<ColumnMeta>,
    batches: Vec<arrow::array::RecordBatch>,
    rows_affected: Option<u64>,
}

async fn ok(connection: &dyn Connection, request: Value) -> Outcome {
    outcome(run(connection, request.clone(), 1000).await, &request)
}

fn outcome(events: Vec<Result<ExecEvent, DriverError>>, request: &Value) -> Outcome {
    let mut columns = None;
    let mut batches = Vec::new();
    let mut rows_affected = None;
    let mut done = false;
    for event in events {
        match event {
            Ok(ExecEvent::Columns(schema)) => columns = Some(schema.columns.clone()),
            Ok(ExecEvent::Batch(batch)) => batches.push(batch),
            Ok(ExecEvent::Notice(_)) => {}
            Ok(ExecEvent::Done { rows_affected: n }) => {
                rows_affected = n;
                done = true;
            }
            Err(error) => panic!("{request} failed: {error}"),
        }
    }
    assert!(done, "{request} never ended");
    Outcome {
        columns: columns.expect("columns"),
        batches,
        rows_affected,
    }
}

impl Outcome {
    fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    fn column(&self, name: &str) -> (usize, &ColumnMeta) {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
            .unwrap_or_else(|| panic!("no column {name} in {:?}", self.names()))
    }

    fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// The cell of `name` in the first row, as text.
    fn text(&self, name: &str) -> Option<String> {
        let (i, meta) = self.column(name);
        let array = self.batches[0].column(i);
        if array.is_null(0) {
            return None;
        }
        Some(match meta.logical {
            LogicalType::Int => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
                .to_string(),
            LogicalType::Float => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_string(),
            LogicalType::Bool => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
                .to_string(),
            LogicalType::TimestampTz => array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0)
                .to_string(),
            _ => array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string(),
        })
    }
}

fn find(database: &str, collection: &str) -> Value {
    json!({"database": database, "command": {"find": collection, "filter": {}, "sort": {"_id": 1}}})
}

#[tokio::test]
async fn every_bson_type_is_a_cell_with_its_type() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    let id = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    let zoo = doc! {
        "_id": id,
        "double": 1.5,
        "string": "ñandú \"quoted\"",
        "object": { "a": 1, "b": [true, null] },
        "array": [1, "two", 3.0],
        "binary": Bson::Binary(mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: vec![0, 1, 254, 255] }),
        "bool": true,
        "date": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
        "null": Bson::Null,
        "regex": Bson::RegularExpression(mongodb::bson::Regex { pattern: "^a.*".into(), options: "i".into() }),
        "javascript": Bson::JavaScriptCode("function () { return 1; }".into()),
        "int": 42_i32,
        "timestamp": Bson::Timestamp(mongodb::bson::Timestamp { time: 1_700_000_000, increment: 7 }),
        "long": 9_007_199_254_740_993_i64,
        "decimal": Bson::Decimal128("12345678901234567890.123456789".parse().unwrap()),
        "minKey": Bson::MinKey,
        "maxKey": Bson::MaxKey,
    };
    seed(&addr, "spelaion_zoo", "zoo", vec![zoo]).await;
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion_zoo").await;
    let result = ok(connection.as_ref(), find("spelaion_zoo", "zoo")).await;

    // name, logical type, native type, cell text
    let expected: &[(&str, LogicalType, &str, Option<&str>)] = &[
        (
            "_id",
            LogicalType::Text,
            "objectId",
            Some("65a1b2c3d4e5f60718293a4b"),
        ),
        ("double", LogicalType::Float, "double", Some("1.5")),
        (
            "string",
            LogicalType::Text,
            "string",
            Some("ñandú \"quoted\""),
        ),
        (
            "object",
            LogicalType::Json,
            "object",
            Some(r#"{"a":1,"b":[true,null]}"#),
        ),
        (
            "array",
            LogicalType::Json,
            "array",
            Some(r#"[1,"two",3.0]"#),
        ),
        (
            "binary",
            LogicalType::Json,
            "binData",
            Some(r#"{"$binary":{"base64":"AAH+/w==","subType":"00"}}"#),
        ),
        ("bool", LogicalType::Bool, "bool", Some("true")),
        (
            "date",
            LogicalType::TimestampTz,
            "date",
            Some("1700000000123000"),
        ),
        ("null", LogicalType::Json, "null", None),
        (
            "regex",
            LogicalType::Json,
            "regex",
            Some(r#"{"$regularExpression":{"pattern":"^a.*","options":"i"}}"#),
        ),
        (
            "javascript",
            LogicalType::Json,
            "javascript",
            Some(r#"{"$code":"function () { return 1; }"}"#),
        ),
        ("int", LogicalType::Int, "int", Some("42")),
        (
            "timestamp",
            LogicalType::Json,
            "timestamp",
            Some(r#"{"$timestamp":{"t":1700000000,"i":7}}"#),
        ),
        ("long", LogicalType::Int, "long", Some("9007199254740993")),
        (
            "decimal",
            LogicalType::Decimal,
            "decimal",
            Some("12345678901234567890.123456789"),
        ),
        (
            "minKey",
            LogicalType::Json,
            "minKey",
            Some(r#"{"$minKey":1}"#),
        ),
        (
            "maxKey",
            LogicalType::Json,
            "maxKey",
            Some(r#"{"$maxKey":1}"#),
        ),
    ];
    let mut names: Vec<&str> = expected.iter().map(|e| e.0).collect();
    names.push("…");
    assert_eq!(result.names(), names);
    for (name, logical, native, text) in expected {
        let (_, meta) = result.column(name);
        assert_eq!(
            (meta.logical, meta.native_type.as_str()),
            (*logical, *native),
            "{name}"
        );
        assert_eq!(result.text(name).as_deref(), *text, "{name}");
    }
    assert_eq!(result.text("…"), None, "nothing left over");
}

#[tokio::test]
async fn a_result_spans_batches_with_the_columns_of_the_first() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    let mut documents: Vec<Document> = (0..2500)
        .map(|i| doc! { "_id": i, "n": i, "label": format!("row {i}") })
        .collect();
    // Past the first batch: a field never seen, and a value of another type.
    documents[2100].insert("late", "here");
    documents[2200].insert("n", "not a number");
    seed(&addr, "spelaion_batches", "many", documents).await;
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion_batches").await;
    let request = find("spelaion_batches", "many");
    let result = outcome(
        run(connection.as_ref(), request.clone(), 1000).await,
        &request,
    );
    assert_eq!(result.names(), ["_id", "n", "label", "…"]);
    assert_eq!(result.batches.len(), 3);
    assert_eq!(result.rows(), 2500);
    let third = &result.batches[2];
    let n = third
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left = third
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Rows 2000.. are the third batch.
    assert_eq!(left.value(100), r#"{"late":"here"}"#);
    assert!(n.is_null(200));
    assert_eq!(left.value(200), r#"{"n":"not a number"}"#);
    assert_eq!(n.value(201), 2201);
    assert!(left.is_null(201));
}

#[tokio::test]
async fn writes_say_how_many_documents_they_changed() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    seed(&addr, "spelaion_writes", "w", vec![]).await;
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion_writes").await;
    let c = connection.as_ref();
    let inserted = ok(
        c,
        json!({"database": "spelaion_writes", "command": {
            "insert": "w", "documents": [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}, {"_id": 3, "v": 3}]
        }}),
    )
    .await;
    assert_eq!(inserted.rows_affected, Some(3));
    // The server's `ok` is a double.
    assert_eq!(inserted.column("ok").1.native_type, "double");
    assert_eq!(inserted.text("ok").as_deref(), Some("1"));
    let updated = ok(c, json!({"database": "spelaion_writes", "command": {
        "update": "w", "updates": [{"q": {"v": {"$gt": 1}}, "u": {"$set": {"seen": true}}, "multi": true}]
    }})).await;
    assert_eq!(updated.rows_affected, Some(2));
    let deleted = ok(
        c,
        json!({"database": "spelaion_writes", "command": {
            "delete": "w", "deletes": [{"q": {"_id": 1}, "limit": 1}]
        }}),
    )
    .await;
    assert_eq!(deleted.rows_affected, Some(1));
    // A read has no count.
    let read = ok(c, find("spelaion_writes", "w")).await;
    assert_eq!(read.rows_affected, None);
    assert_eq!(read.rows(), 2);
    // Extended JSON in the request keeps its types: a long stays a long.
    ok(c, json!({"database": "spelaion_writes", "command": {
        "insert": "w", "documents": [{"_id": 9, "big": {"$numberLong": "9007199254740993"}, "when": {"$date": {"$numberLong": "1700000000000"}}}]
    }})).await;
    let back = ok(
        c,
        json!({"database": "spelaion_writes", "command": {"find": "w", "filter": {"_id": 9}}}),
    )
    .await;
    assert_eq!(back.column("big").1.native_type, "long");
    assert_eq!(back.text("big").as_deref(), Some("9007199254740993"));
    assert_eq!(back.column("when").1.native_type, "date");
}

#[tokio::test]
async fn the_server_refuses_with_its_code_and_the_session_goes_on() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion").await;
    let events = run(
        connection.as_ref(),
        json!({"database": "spelaion", "command": {"find": "x", "filter": {"$bogus": 1}}}),
        100,
    )
    .await;
    let error = events.into_iter().find_map(Result::err).expect("refused");
    match error {
        DriverError::Database { code, message, .. } => {
            assert_eq!(code.as_deref(), Some("BadValue"), "{message}");
        }
        other => panic!("not the server's refusal: {other:?}"),
    }
    ok(
        connection.as_ref(),
        json!({"database": "admin", "command": {"ping": 1}}),
    )
    .await;
}

#[tokio::test]
async fn a_request_that_is_not_a_command_is_refused_before_it_is_sent() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion").await;
    for (request, why) in [
        (json!(["ping"]), "not an object"),
        (json!({"command": {"ping": 1}}), "names no database"),
        (
            json!({"database": "", "command": {"ping": 1}}),
            "names no database",
        ),
        (json!({"database": "admin"}), "no command document"),
        (
            json!({"database": "admin", "command": {}}),
            "command is empty",
        ),
        (
            json!({"database": "admin", "command": {"ping": 1}, "also": 1}),
            "a field `also`",
        ),
    ] {
        let events = run(connection.as_ref(), request.clone(), 10).await;
        let error = events
            .into_iter()
            .find_map(Result::err)
            .expect("refused")
            .to_string();
        assert!(error.contains(why), "{request}: {error}");
    }
    // A statement's text is never run.
    let error = connection
        .execute("db.users.find()", &ExecOptions::default())
        .await
        .err()
        .expect("text refused")
        .to_string();
    assert!(error.contains("runs requests"), "{error}");
}

#[tokio::test]
async fn the_catalog_lists_databases_collections_and_indexes() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    seed(
        &addr,
        "spelaion_catalog",
        "people",
        vec![doc! { "_id": 1, "email": "a@b.c" }],
    )
    .await;
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion_catalog").await;
    let c = connection.as_ref();
    ok(c, json!({"database": "spelaion_catalog", "command": {
        "createIndexes": "people", "indexes": [{"key": {"email": 1}, "name": "email_1", "unique": true}]
    }})).await;
    // A view is a collection too, said so in its comment.
    let _ = run(
        c,
        json!({"database": "spelaion_catalog", "command": {"drop": "adults"}}),
        10,
    )
    .await;
    ok(
        c,
        json!({"database": "spelaion_catalog", "command": {
            "create": "adults", "viewOn": "people", "pipeline": []
        }}),
    )
    .await;

    let schemas = c.list_schemas().await.unwrap();
    let catalog = schemas
        .iter()
        .find(|s| s.name == "spelaion_catalog")
        .expect("the database");
    assert!(!catalog.system);
    assert!(schemas.iter().any(|s| s.name == "admin" && s.system));
    assert!(
        c.list_databases()
            .await
            .unwrap()
            .contains(&"spelaion_catalog".to_string())
    );

    let objects = c.list_objects("spelaion_catalog").await.unwrap();
    let people = objects.iter().find(|o| o.name == "people").expect("people");
    assert_eq!(people.kind, ObjectKind::Collection);
    assert_eq!(people.comment, None);
    assert_eq!(people.estimated_rows, None);
    let adults = objects.iter().find(|o| o.name == "adults").expect("adults");
    assert_eq!(adults.comment.as_deref(), Some("view"));

    let table = c
        .describe_table("spelaion_catalog", "people")
        .await
        .unwrap();
    assert!(table.columns.is_empty());
    assert!(table.primary_key.is_empty());
    let id = table
        .indexes
        .iter()
        .find(|i| i.name == "_id_")
        .expect("_id_");
    assert!(id.primary);
    let email = table
        .indexes
        .iter()
        .find(|i| i.name == "email_1")
        .expect("email_1");
    assert!(email.unique && !email.primary);
    assert!(
        email.definition.contains(r#""key":{"email":1}"#),
        "{}",
        email.definition
    );
    assert!(
        c.describe_table("spelaion_catalog", "adults")
            .await
            .unwrap()
            .indexes
            .is_empty()
    );
    assert!(c.list_columns("spelaion_catalog").await.unwrap().is_empty());

    assert_eq!(
        c.current_database().await.unwrap().as_deref(),
        Some("spelaion_catalog")
    );
    c.select_database("other").await.unwrap();
    assert_eq!(
        c.current_database().await.unwrap().as_deref(),
        Some("other")
    );
    assert!(!c.server_version().await.unwrap().is_empty());
    assert!(matches!(
        c.keep_alive().await.unwrap(),
        spelaion_driver_api::KeepAlive::Alive
    ));
}

#[tokio::test]
async fn connecting_refuses_what_it_cannot_honour() {
    let Some(addr) = server("SPELAION_TEST_MONGO") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let driver = plugin.driver("mongodb");

    let mut wrong_password = params(&addr, "spelaion");
    wrong_password.password = Some(Secret::new("not-the-password"));
    let error = driver
        .connect(&wrong_password)
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("refused the login"), "{error}");
    assert!(!error.contains("not-the-password"), "{error}");

    let mut unknown = params(&addr, "spelaion");
    unknown
        .options
        .insert("srvServiceName".into(), "other".into());
    let error = driver
        .connect(&unknown)
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(
        error.contains("no connection setting `srvServiceName`"),
        "{error}"
    );

    let mut mechanism = params(&addr, "spelaion");
    mechanism
        .options
        .insert("authMechanism".into(), "PLAIN".into());
    let error = driver
        .connect(&mechanism)
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("SCRAM-SHA-1 or SCRAM-SHA-256"), "{error}");

    // The settings it knows are used.
    let mut source = params(&addr, "spelaion");
    source.options.insert("authSource".into(), "admin".into());
    source
        .options
        .insert("authMechanism".into(), "SCRAM-SHA-256".into());
    driver.connect(&source).await.expect("connects");
    let mut elsewhere = params(&addr, "spelaion");
    elsewhere
        .options
        .insert("authSource".into(), "spelaion".into());
    assert!(
        driver.connect(&elsewhere).await.is_err(),
        "the user is in admin"
    );

    let mut read_only = params(&addr, "spelaion");
    read_only.read_only = true;
    let error = driver
        .connect(&read_only)
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("no read-only mode"), "{error}");

    let mut tunnelled = params(&addr, "spelaion");
    tunnelled.connect_addr = Some(addr.parse().unwrap());
    tunnelled.host = "db.example.internal".into();
    // Through a "tunnel" (its address given apart from the name), it connects to that address.
    tunnelled.tls = spelaion_driver_api::TlsMode::Disable;
    driver
        .connect(&tunnelled)
        .await
        .expect("connects through the address given");
}

#[tokio::test]
async fn routed_tls_checks_logical_hostname_authority_and_replica_set() {
    if server("SPELAION_TEST_MONGO").is_none() {
        return;
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        let plugin = Plugin::new(PROGRAM);
        let driver = plugin.driver("mongodb");
        let mut params = spelaion_driver_api::ConnectParams {
            host: "mongo.example.test".into(),
            port: 27017,
            connect_addr: Some(
                std::env::var("SPELAION_TEST_MONGO_TLS")
                    .unwrap_or_else(|_| "127.0.0.1:27020".into())
                    .parse()
                    .expect("TLS host:port"),
            ),
            ca_certificate: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testing/tls/ca.crt"),
            ),
            tls: spelaion_driver_api::TlsMode::VerifyFull,
            connect_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        params
            .options
            .insert("replicaSet".into(), "atlas-test".into());
        for mode in [
            spelaion_driver_api::TlsMode::VerifyFull,
            spelaion_driver_api::TlsMode::VerifyCa,
        ] {
            params.tls = mode;
            let connection = driver
                .connect(&params)
                .await
                .expect("verified TLS through the supplied socket");
            ok(
                connection.as_ref(),
                json!({"database":"admin","command":{"ping":1}}),
            )
            .await;
            drop(connection);
            params.host = "wrong.example.test".into();
            assert!(
                driver.connect(&params).await.is_err(),
                "wrong certificate name must fail"
            );
            params.host = "mongo.example.test".into();
        }
        params.ca_certificate = None;
        assert!(
            driver.connect(&params).await.is_err(),
            "an untrusted authority must fail"
        );
        params.ca_certificate =
            Some(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testing/tls/ca.crt"));
        params
            .options
            .insert("replicaSet".into(), "wrong-set".into());
        assert!(
            driver.connect(&params).await.is_err(),
            "wrong replica set must fail"
        );
    })
    .await
    .expect("TLS checks end in time");
}

#[tokio::test]
async fn ferretdb_reads_and_writes_as_mongodb_does() {
    let Some(addr) = server("SPELAION_TEST_FERRETDB") else {
        return;
    };
    seed(
        &addr,
        "spelaion_ferret",
        "docs",
        vec![
            doc! { "_id": 1, "name": "one", "n": 1, "tags": ["a"] },
            doc! { "_id": 2, "name": "two", "n": 2.5 },
        ],
    )
    .await;
    let plugin = Plugin::new(PROGRAM);
    let connection = connect(&plugin, &addr, "spelaion_ferret").await;
    let c = connection.as_ref();
    let result = ok(c, find("spelaion_ferret", "docs")).await;
    assert_eq!(result.names(), ["_id", "name", "n", "tags", "…"]);
    assert_eq!(result.rows(), 2);
    assert_eq!(result.column("n").1.native_type, "mixed");
    let inserted = ok(c, json!({"database": "spelaion_ferret", "command": {"insert": "docs", "documents": [{"_id": 3}]}})).await;
    assert_eq!(inserted.rows_affected, Some(1));
    let aggregated = ok(
        c,
        json!({"database": "spelaion_ferret", "command": {
            "aggregate": "docs", "pipeline": [{"$count": "total"}], "cursor": {}
        }}),
    )
    .await;
    assert_eq!(aggregated.text("total").as_deref(), Some("3"));
    let objects = c.list_objects("spelaion_ferret").await.unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o.name == "docs" && o.kind == ObjectKind::Collection)
    );
    let table = c.describe_table("spelaion_ferret", "docs").await.unwrap();
    assert!(table.indexes.iter().any(|i| i.name == "_id_"));
    assert!(!c.server_version().await.unwrap().is_empty());
}
