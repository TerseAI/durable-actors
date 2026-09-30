use super::*;
use crate::sandbox::source::{SourceArchive, SourceObject};
use std::sync::Mutex;

struct Api {
    calls: Mutex<Vec<(Method, String, Option<Value>)>>,
    status: &'static str,
}
#[async_trait]
impl BuildApi for Api {
    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        self.calls
            .lock()
            .unwrap()
            .push((method.clone(), path.into(), body));
        Ok(if method == Method::POST {
            json!({"metadata":{"build":{"id":"build-id"}}})
        } else {
            json!({"status":self.status})
        })
    }
}
struct Results(Value);
#[async_trait]
impl BuildResults for Results {
    async fn read(&self, bucket: &str, object: &str) -> Result<Value> {
        assert_eq!(bucket, "code");
        assert_eq!(object, "artifacts/run/result.json");
        Ok(self.0.clone())
    }
}
fn request() -> BuildRequest {
    BuildRequest {
        source: SourceArchive {
            sha256: "a".repeat(64),
            entrypoint: "src/$BUILD_ID.ts".into(),
            object: Some(SourceObject {
                bucket: "source".into(),
                name: "project.zip".into(),
                generation: "7".into(),
            }),
        },
        bucket: "code".into(),
        artifact_prefix: "artifacts/run/".into(),
        dependency_prefix: "deps/project/".into(),
        access_token: "scoped-token".into(),
    }
}
fn runner(status: &'static str, result: Value) -> (CloudBuild, Arc<Api>) {
    let api = Arc::new(Api {
        calls: Mutex::new(vec![]),
        status,
    });
    (
        CloudBuild {
            config: CloudBuildConfig {
                project: "test-project".into(),
                region: "us-west4".into(),
                service_account: "build@test-project.iam.gserviceaccount.com".into(),
                machine_type: "E2_HIGHCPU_8".into(),
            },
            image: "registry/runtime@sha256:abc".into(),
            api: api.clone(),
            results: Arc::new(Results(result)),
        },
        api,
    )
}
#[tokio::test]
async fn submits_scoped_build_and_reads_the_published_bundle() -> Result<()> {
    let (runner, api) = runner(
        "SUCCESS",
        json!({"manifest":{"bucket":"code","files":[]},"contract":{"version":1,"actors":[]},"timings":{"compileMs":123},"dependencyCacheHit":true}),
    );
    let reply = runner.build("north-america-west", &request()).await?;
    assert!(reply.dependency_cache_hit);
    assert_eq!(reply.timings["compileMs"], 123);
    let calls = api.calls.lock().unwrap();
    assert_eq!(
        calls[0].1,
        "projects/test-project/locations/us-west4/builds"
    );
    let body = calls[0].2.as_ref().unwrap();
    assert_eq!(body["options"]["machineType"], "E2_HIGHCPU_8");
    assert_eq!(
        body["serviceAccount"],
        "projects/test-project/serviceAccounts/build@test-project.iam.gserviceaccount.com"
    );
    assert_eq!(body["steps"][0]["entrypoint"], "python3");
    let environment = body["steps"][0]["env"][0].as_str().unwrap();
    assert!(environment.contains("$$BUILD_ID.ts"));
    let submitted: Value = serde_json::from_str(
        environment
            .strip_prefix("DURABLE_ACTORS_BUILD_REQUEST=")
            .unwrap()
            .replace("$$", "$")
            .as_str(),
    )?;
    assert_eq!(submitted["source"]["object"]["generation"], "7");
    assert_eq!(submitted["accessToken"], "scoped-token");
    assert_eq!(
        calls[1].1,
        "projects/test-project/locations/us-west4/builds/build-id"
    );
    Ok(())
}
#[tokio::test]
async fn reports_compiler_failure_with_build_identity() {
    let (runner, _) = runner("FAILURE", json!({"error":"Cannot resolve actor import"}));
    let error = runner
        .build("region", &request())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("build-id") && error.contains("Cannot resolve actor import"),
        "{error}"
    );
}
#[test]
fn validates_supported_machine_types() {
    let (mut runner, _) = runner("SUCCESS", json!({}));
    assert!(runner.config.validate().is_ok());
    runner.config.machine_type = "unknown".into();
    assert!(runner.config.validate().is_err());
}

#[tokio::test(start_paused = true)]
async fn cancels_a_build_that_exceeds_the_deadline() {
    let (runner, api) = runner("WORKING", json!({}));
    let error = runner
        .build("region", &request())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("exceeded its deadline"));
    let calls = api.calls.lock().unwrap();
    let last = calls.last().unwrap();
    assert_eq!(last.0, Method::POST);
    assert_eq!(
        last.1,
        "projects/test-project/locations/us-west4/builds/build-id:cancel"
    );
}
