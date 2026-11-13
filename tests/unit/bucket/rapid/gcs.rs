use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

#[tokio::test]
async fn rapid_uploads_hand_whole_records_to_the_sdk_and_only_succeed_after_flush() -> Result<()> {
    let bytes = Bytes::from(
        (0..5 * 1024 * 1024 + 17)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>(),
    );
    for (fail_append, fail_flush) in [(false, false), (true, false), (false, true)] {
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicU64::new(0));
        let mut writer = GcsWriter(AppendableObjectWriter::new(UploadStub {
            chunks: chunks.clone(),
            flushes: flushes.clone(),
            fail_append,
            fail_flush,
            persisted: 0,
        }));
        let result = writer.append_and_flush(bytes.clone()).await;
        if fail_append || fail_flush {
            assert!(result.is_err());
        } else {
            assert_eq!(result?, bytes.len() as u64);
        }
        assert_eq!(flushes.load(Ordering::SeqCst), u64::from(!fail_append));
        let chunks = chunks.lock().unwrap();
        assert_eq!(chunks.len(), usize::from(!fail_append));
        assert!(chunks.iter().all(|chunk| *chunk == bytes));
    }
    Ok(())
}

#[derive(Debug)]
struct UploadStub {
    chunks: Arc<Mutex<Vec<Bytes>>>,
    flushes: Arc<AtomicU64>,
    fail_append: bool,
    fail_flush: bool,
    persisted: i64,
}

impl google_cloud_storage::stub::AppendableObjectWriter for UploadStub {
    async fn append(&mut self, chunk: Bytes) -> google_cloud_storage::Result<()> {
        if self.fail_append {
            return Err(google_cloud_storage::Error::io("injected append failure"));
        }
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    async fn flush(&mut self) -> google_cloud_storage::Result<i64> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        if self.fail_flush {
            return Err(google_cloud_storage::Error::io("injected flush failure"));
        }
        self.persisted = self
            .chunks
            .lock()
            .unwrap()
            .iter()
            .map(|chunk| chunk.len() as i64)
            .sum();
        Ok(self.persisted)
    }

    async fn finalize(
        mut self,
    ) -> google_cloud_storage::Result<google_cloud_storage::model::Object> {
        Ok(google_cloud_storage::model::Object::default().set_size(self.flush().await?))
    }

    async fn close(mut self) -> google_cloud_storage::Result<i64> {
        self.flush().await
    }

    fn generation(&self) -> i64 {
        1
    }

    fn persisted_size(&self) -> i64 {
        self.persisted
    }
}

#[tokio::test]
async fn startup_rejects_archive_lifecycle_deletion() -> Result<()> {
    use serde_json::json;
    for condition in [
        json!({}),
        json!({"ageDays": 365}),
        json!({"matchesPrefix": [""]}),
        json!({"matchesPrefix": ["durable-actors-"]}),
        json!({"matchesPrefix": ["durable-actors-v3-snapshots-"]}),
        json!({"matchesPrefix": ["durable-actors-v3-snapshots-actor~log~"]}),
        json!({"matchesPrefix": ["tmp/", "durable-actors-v3-snapshots-"]}),
        json!({"ageDays": 30, "isLive": true, "matchesSuffix": [".batch"]}),
        json!({"matchesStorageClass": ["NEARLINE"], "daysSinceCustomTime": 90}),
        json!({"isLive": false, "numNewerVersions": 2}),
    ] {
        let lifecycle = json!({"rule": [{"action": {"type": "Delete"}, "condition": condition}]});
        let error = validate_startup(Some(lifecycle), "STANDARD", None, false)
            .await
            .expect_err(&format!("accepted archive deletion: {condition}"));
        assert!(error.to_string().contains("archive"), "{error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn startup_accepts_archive_rules_with_disjoint_prefixes_and_known_non_delete_actions()
-> Result<()> {
    use serde_json::json;
    for lifecycle in [
        None,
        Some(json!({})),
        Some(json!({"rule": []})),
        Some(json!({"rule": [
            {"action": {"type": "Delete"}, "condition": {"ageDays": 7, "matchesPrefix": ["tmp/", "durable-actors-v3-logs-"]}},
            {"action": {"type": "Delete"}, "condition": {"matchesPrefix": ["Durable-actors-v3-snapshots-", "durable-actors-v3-snapshots/"]}},
            {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"}, "condition": {"ageDays": 30}},
            {"action": {"type": "AbortIncompleteMultipartUpload"}, "condition": {"ageDays": 7}}
        ]})),
    ] {
        validate_startup(lifecycle, "STANDARD", None, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn startup_rejects_missing_or_unknown_archive_actions_and_checks_every_rule() -> Result<()> {
    use serde_json::json;
    for rule in [
        json!({}),
        json!({"action": {}}),
        json!({"action": {"type": "Unknown"}}),
        json!({"action": {"type": "Delete"}}),
    ] {
        let lifecycle = json!({"rule": [
            {"action": {"type": "Delete"}, "condition": {"matchesPrefix": ["tmp/"]}}, rule
        ]});
        assert!(
            validate_startup(Some(lifecycle), "STANDARD", None, false)
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn startup_requires_readable_metadata_and_preserves_standard_and_rapid_roles() -> Result<()> {
    for class in ["RAPID", "NEARLINE", ""] {
        assert!(validate_startup(None, class, None, false).await.is_err());
    }
    for status in [
        tonic::Code::PermissionDenied,
        tonic::Code::NotFound,
        tonic::Code::Unauthenticated,
        tonic::Code::DataLoss,
    ] {
        assert!(
            validate_startup(None, "STANDARD", Some(status), false)
                .await
                .is_err()
        );
    }
    assert!(
        validate_startup(None, "STANDARD", None, true)
            .await
            .is_err()
    );
    Ok(())
}

async fn validate_startup(
    lifecycle: Option<serde_json::Value>,
    storage_class: &str,
    failure: Option<tonic::Code>,
    unsafe_rapid: bool,
) -> Result<()> {
    use axum::response::IntoResponse;
    use google_cloud_auth::credentials::anonymous;
    use google_cloud_storage::client::{Storage, StorageControl};
    let metadata = MetadataService {
        archive: MetadataBucket {
            storage_class: storage_class.into(),
            lifecycle: lifecycle.map(serde_json::from_value).transpose()?,
            placement: None,
        },
        failure,
        unsafe_rapid,
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let routes = axum::Router::new().route(
        "/google.storage.v2.Storage/GetBucket",
        axum::routing::post(move |request: axum::extract::Request| {
            let metadata = metadata.clone();
            let seen = seen.clone();
            async move {
                if metadata.failure == Some(tonic::Code::DataLoss) {
                    seen.lock().unwrap().push("malformed response".into());
                    return (
                        [("content-type", "application/grpc")],
                        vec![0u8, 0, 0, 0, 1, 255],
                    )
                        .into_response();
                }
                tonic::server::Grpc::new(
                    tonic_prost::ProstCodec::<MetadataBucket, MetadataRequest>::default(),
                )
                .unary(RecordingMetadata { metadata, seen }, request)
                .await
                .map(axum::body::Body::new)
                .into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let clients = GcsClients {
        transfers: Arc::new(tokio::sync::Semaphore::new(2)),
        storage: Storage::builder()
            .with_endpoint(&endpoint)
            .with_credentials(anonymous::Builder::new().build())
            .build()
            .await?,
        control: StorageControl::builder()
            .with_endpoint(&endpoint)
            .with_credentials(anonymous::Builder::new().build())
            .build()
            .await?,
    };
    let config: SnapshotStoreConfig = serde_json::from_value(serde_json::json!({
        "type": "rapid", "archive_bucket": "archive",
        "buckets": [{"bucket": "rapid", "zone": "us-west4-a"}]
    }))?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        RapidSnapshots::validate_gcs(&config, clients),
    )
    .await;
    server.abort();
    let result = result?;
    let requests = requests.lock().unwrap();
    if failure == Some(tonic::Code::DataLoss) {
        assert_eq!(*requests, ["malformed response"]);
        assert!(result.is_err());
        return result;
    }
    assert_eq!(
        requests.first().map(String::as_str),
        Some("projects/_/buckets/archive")
    );
    if result.is_err() && !unsafe_rapid {
        assert_eq!(*requests, ["projects/_/buckets/archive"]);
    }
    if result.is_ok() {
        assert_eq!(
            *requests,
            ["projects/_/buckets/archive", "projects/_/buckets/rapid"]
        );
    }
    result
}

#[derive(Clone)]
struct MetadataService {
    archive: MetadataBucket,
    failure: Option<tonic::Code>,
    unsafe_rapid: bool,
}

struct RecordingMetadata {
    metadata: MetadataService,
    seen: Arc<Mutex<Vec<String>>>,
}

impl tonic::server::UnaryService<MetadataRequest> for RecordingMetadata {
    type Response = MetadataBucket;
    type Future =
        std::future::Ready<std::result::Result<tonic::Response<MetadataBucket>, tonic::Status>>;

    fn call(&mut self, request: tonic::Request<MetadataRequest>) -> Self::Future {
        let name = request.into_inner().name;
        self.seen.lock().unwrap().push(name.clone());
        let result = match name.as_str() {
            "projects/_/buckets/archive" => match self.metadata.failure {
                Some(code) => Err(tonic::Status::new(code, "metadata unavailable")),
                None => Ok(self.metadata.archive.clone()),
            },
            "projects/_/buckets/rapid" => Ok(MetadataBucket {
                storage_class: "RAPID".into(),
                placement: Some(MetadataPlacement {
                    locations: vec!["us-west4-a".into()],
                }),
                lifecycle: self.metadata.unsafe_rapid.then(|| {
                    serde_json::from_value(
                        serde_json::json!({"rule": [{"action": {"type": "Delete"}}]}),
                    )
                    .unwrap()
                }),
            }),
            _ => Err(tonic::Status::invalid_argument("unexpected bucket")),
        };
        std::future::ready(result.map(tonic::Response::new))
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct MetadataRequest {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MetadataBucket {
    #[prost(string, tag = "7")]
    storage_class: String,
    #[prost(message, optional, tag = "10")]
    lifecycle: Option<MetadataLifecycle>,
    #[prost(message, optional, tag = "26")]
    placement: Option<MetadataPlacement>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MetadataPlacement {
    #[prost(string, repeated, tag = "1")]
    locations: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message, serde::Deserialize)]
struct MetadataLifecycle {
    #[prost(message, repeated, tag = "1")]
    #[serde(default)]
    rule: Vec<MetadataRule>,
}

#[derive(Clone, PartialEq, prost::Message, serde::Deserialize)]
struct MetadataRule {
    #[prost(message, optional, tag = "1")]
    action: Option<MetadataAction>,
    #[prost(message, optional, tag = "2")]
    condition: Option<MetadataCondition>,
}

#[derive(Clone, PartialEq, prost::Message, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct MetadataAction {
    #[prost(string, tag = "1")]
    r#type: String,
    #[prost(string, tag = "2")]
    storage_class: String,
}

#[derive(Clone, PartialEq, prost::Message, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct MetadataCondition {
    #[prost(int32, optional, tag = "1")]
    age_days: Option<i32>,
    #[prost(bool, optional, tag = "3")]
    is_live: Option<bool>,
    #[prost(int32, optional, tag = "4")]
    num_newer_versions: Option<i32>,
    #[prost(string, repeated, tag = "5")]
    matches_storage_class: Vec<String>,
    #[prost(int32, optional, tag = "7")]
    days_since_custom_time: Option<i32>,
    #[prost(string, repeated, tag = "11")]
    matches_prefix: Vec<String>,
    #[prost(string, repeated, tag = "12")]
    matches_suffix: Vec<String>,
}
