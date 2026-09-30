use super::*;

#[tokio::test]
async fn replenishment_reaps_abandoned_workers_without_deleting_another_active_build() -> Result<()>
{
    use axum::{
        Json, Router,
        extract::{Path, Query},
        routing::get,
    };
    let deleted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let removals = deleted.clone();
    let routes = Router::new().route("/api/v1/namespaces/sandboxes/pods", get(|Query(query): Query<BTreeMap<String, String>>| async move {
        let items = if query["labelSelector"].contains("build-state=idle") { vec![] } else {
            [("expired", "Running", 901), ("failed", "Failed", 30), ("active", "Running", 300)].into_iter().map(|(name, phase, age)| json!({
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "uid": format!("{name}-uid"), "resourceVersion": "7",
                "creationTimestamp": (k8s_openapi::chrono::Utc::now() - k8s_openapi::chrono::Duration::seconds(age)).to_rfc3339()}, "status": {"phase": phase}
            })).collect()
        };
        Json(json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items}))
    })).route("/api/v1/namespaces/sandboxes/pods/{name}", axum::routing::delete(move |Path(name): Path<String>, Json(body): Json<serde_json::Value>| {
        let removals = removals.clone();
        async move {
            assert_eq!(body["preconditions"]["uid"], format!("{name}-uid"));
            assert_eq!(body["preconditions"]["resourceVersion"], "7");
            removals.lock().unwrap().push(name);
            Json(json!({"apiVersion": "v1", "kind": "Status", "status": "Success"}))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let pool = WorkerPool::new(
        Arc::new(Kubernetes::new(
            Client::try_from(config)?,
            GkeConfig {
                namespace: "sandboxes".into(),
                zones: BTreeMap::new(),
                public_origin: "https://actors.example.com".into(),
                artifact_bucket: "code".into(),
            },
        )),
        format!("runtime@sha256:{}", "a".repeat(64)),
        BuilderConfig {
            idle: 0,
            concurrent: 1,
            resources: ResourceLimits {
                cpu_millis: 1000,
                memory_mib: 1024,
            },
        },
    )?;
    pool.replenish().await?;
    assert_eq!(*deleted.lock().unwrap(), vec!["expired", "failed"]);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn concurrent_builds_claim_distinct_workers_and_failures_delete_the_claimed_pod() -> Result<()>
{
    use axum::{Json, Router, extract::Path, http::StatusCode, routing::get};
    use std::sync::Mutex;
    let claimed = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let pods: Vec<_> = ["one", "two", "three"].iter().map(|name| json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "uid": format!("{name}-uid"), "resourceVersion": "1", "creationTimestamp": k8s_openapi::chrono::Utc::now().to_rfc3339()},
        "spec": {"containers": [{"name": "runtime"}], "nodeSelector": {"topology.kubernetes.io/zone": "us-west4-a"}},
        "status": {"phase": "Running", "podIP": "127.0.0.1", "conditions": [{"type": "Ready", "status": "True"}]}
    })).collect();
    let listed = pods.clone();
    let claims = claimed.clone();
    let removals = deleted.clone();
    let routes = Router::new()
        .route("/api/v1/namespaces/sandboxes/pods", get(move || {
            let items = listed.clone();
            async move { Json(json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items})) }
        }))
        .route("/api/v1/namespaces/sandboxes/pods/{name}", axum::routing::patch(move |Path(name): Path<String>, Json(patch): Json<serde_json::Value>| {
            let pods = pods.clone();
            let claims = claims.clone();
            async move {
                assert_eq!(patch["metadata"]["resourceVersion"], "1");
                assert_eq!(patch["metadata"]["labels"]["terse.ai/build-state"], "busy");
                if claims.lock().unwrap().insert(name.clone()) {
                    (StatusCode::OK, Json(pods.into_iter().find(|pod| pod["metadata"]["name"] == name).unwrap()))
                } else {
                    (StatusCode::CONFLICT, Json(json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","message":"already claimed","code":409})))
                }
            }
        }).delete(move |Path(name): Path<String>, Json(body): Json<serde_json::Value>| {
            let removals = removals.clone();
            async move {
                assert_eq!(body["preconditions"]["uid"], format!("{name}-uid"));
                removals.lock().unwrap().push(name);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let pool = WorkerPool::new(
        Arc::new(Kubernetes::new(
            Client::try_from(config)?,
            GkeConfig {
                namespace: "sandboxes".into(),
                zones: BTreeMap::from([("west".into(), "us-west4-a".into())]),
                public_origin: "https://actors.example.com".into(),
                artifact_bucket: "code".into(),
            },
        )),
        format!("runtime@sha256:{}", "a".repeat(64)),
        BuilderConfig {
            idle: 1,
            concurrent: 2,
            resources: ResourceLimits {
                cpu_millis: 1000,
                memory_mib: 1024,
            },
        },
    )?;
    let (one, two) = tokio::join!(pool.reserve("west"), pool.reserve("west"));
    let (one, reused_one) = one?;
    let (two, reused_two) = two?;
    assert!(reused_one && reused_two);
    assert_ne!(one.name_any(), two.name_any());
    let result = pool
        .build(
            "west",
            &WorkerRequest {
                source: crate::sandbox::source::SourceArchive {
                    sha256: "a".repeat(64),
                    entrypoint: "src/actor.ts".into(),
                    object: None,
                },
                bucket: "code".into(),
                artifact_prefix: "artifacts/test/".into(),
                dependency_prefix: "deps/test/".into(),
                access_token: "scoped".into(),
            },
        )
        .await;
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("worker token missing")
    );
    assert_eq!(*deleted.lock().unwrap(), vec!["three"]);
    server.abort();
    Ok(())
}

#[test]
fn source_worker_uses_the_shared_image_and_cannot_access_cluster_credentials() -> Result<()> {
    let pod = worker_pod(
        "runtime@sha256:abc",
        "us-west4-a",
        "owner",
        &ResourceLimits {
            cpu_millis: 1000,
            memory_mib: 1024,
        },
    )?;
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(spec.automount_service_account_token, Some(false));
    assert_eq!(spec.active_deadline_seconds, Some(900));
    assert_eq!(
        spec.containers[0].image.as_deref(),
        Some("runtime@sha256:abc")
    );
    assert_eq!(
        spec.containers[0]
            .security_context
            .as_ref()
            .unwrap()
            .read_only_root_filesystem,
        Some(true)
    );
    assert_eq!(
        spec.containers[0].command.as_ref().unwrap(),
        &["python3", "/opt/durable-actors/source-build.py"]
    );
    assert_eq!(pod.metadata.labels.unwrap()["terse.ai/build-state"], "idle");
    Ok(())
}
