use super::*;

#[test]
fn customer_pod_enforces_isolation_and_allows_node_scale_down() -> Result<()> {
    let request = CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: "warm-one".into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 2000,
            memory_mib: 4096,
        },
    };
    let pod = spare_pod(
        &request,
        &[
            "us-west4-a".into(),
            "us-west4-b".into(),
            "us-west4-c".into(),
        ],
        "test-token",
        false,
    )?;
    assert_eq!(
        pod.metadata
            .annotations
            .as_ref()
            .and_then(
                |annotations| annotations.get("cluster-autoscaler.kubernetes.io/safe-to-evict")
            )
            .map(String::as_str),
        Some("true"),
    );
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(spec.automount_service_account_token, Some(false));
    let affinity = spec
        .affinity
        .as_ref()
        .unwrap()
        .node_affinity
        .as_ref()
        .unwrap()
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .unwrap();
    assert_eq!(
        affinity.node_selector_terms[0]
            .match_expressions
            .as_ref()
            .unwrap()[0]
            .values
            .as_ref()
            .unwrap(),
        &["us-west4-a", "us-west4-b", "us-west4-c"]
    );
    let spread = &spec.topology_spread_constraints.as_ref().unwrap()[0];
    assert_eq!(spread.topology_key, "topology.kubernetes.io/zone");
    assert_eq!(spread.when_unsatisfiable, "ScheduleAnyway");
    assert_eq!(
        spec.containers[0]
            .resources
            .as_ref()
            .unwrap()
            .limits
            .as_ref()
            .unwrap()["memory"]
            .0,
        "4096Mi"
    );
    assert_eq!(
        spec.containers[0]
            .security_context
            .as_ref()
            .unwrap()
            .allow_privilege_escalation,
        Some(false)
    );
    Ok(())
}

#[test]
fn only_metered_pods_hold_the_usage_finalizer() -> Result<()> {
    let request = CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: "warm-one".into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits::default(),
    };
    let zones = ["us-west4-a".to_string()];
    assert_eq!(
        spare_pod(&request, &zones, "token", true)?
            .metadata
            .finalizers,
        Some(vec![USAGE_FINALIZER.to_string()])
    );
    assert_eq!(
        spare_pod(&request, &zones, "token", false)?
            .metadata
            .finalizers,
        None
    );
    Ok(())
}

#[tokio::test]
async fn stopping_keeps_the_termination_record_and_retirement_releases_it() -> Result<()> {
    use axum::{Json, Router, body::Bytes, extract::Path, http::Method, routing::any};
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods/{name}",
        any(
            move |method: Method, Path(name): Path<String>, body: Bytes| async move {
                let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                recorded
                    .lock()
                    .unwrap()
                    .push((method.to_string(), name, body));
                Json(pod("metered", "Running", None))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
        },
        true,
    );
    let spare = SpareHandle {
        name: "metered".into(),
        resource_id: "sandboxes/metered/metered-uid".into(),
        route: String::new(),
        canonical_region: String::new(),
        control_route: String::new(),
        control_token: String::new(),
    };
    let stopped = cluster.stop_spare(&spare).await;
    let stop_calls = std::mem::take(&mut *calls.lock().unwrap());
    let retired = cluster.retire_spare(&spare).await;
    let retire_calls = std::mem::take(&mut *calls.lock().unwrap());
    server.abort();
    stopped?;
    retired?;
    assert_eq!(stop_calls.len(), 1);
    assert_eq!(
        (stop_calls[0].0.as_str(), stop_calls[0].1.as_str()),
        ("DELETE", "metered")
    );
    assert_eq!(stop_calls[0].2["preconditions"]["uid"], "metered-uid");
    assert_eq!(
        retire_calls
            .iter()
            .map(|(method, name, _)| (method.as_str(), name.as_str()))
            .collect::<Vec<_>>(),
        [("PATCH", "metered"), ("DELETE", "metered")]
    );
    assert_eq!(
        retire_calls[0].2,
        json!([
            {"op":"test","path":"/metadata/uid","value":"metered-uid"},
            {"op":"add","path":"/metadata/finalizers","value":[]}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn retirement_recovers_a_lost_create_reply_and_uses_a_uid_precondition() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let deleted = Arc::new(std::sync::Mutex::new(None));
    let recorded = deleted.clone();
    let pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"reserved-pod","namespace":"sandboxes","uid":"created-uid","labels":{"app.kubernetes.io/managed-by":"terse"}}});
    let patched = pod.clone();
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods/reserved-pod",
        get(move || async move { Json(pod) })
            .patch(move || async move { Json(patched) })
            .delete(move |Json(body): Json<serde_json::Value>| async move {
                *recorded.lock().unwrap() = Some(body);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
        },
        false,
    );
    let result = cluster
        .retire_spare(&SpareHandle {
            name: "reserved-pod".into(),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        })
        .await;
    server.abort();
    result?;
    assert_eq!(
        deleted.lock().unwrap().as_ref().unwrap()["preconditions"]["uid"],
        "created-uid"
    );
    Ok(())
}

#[tokio::test]
async fn stopped_spares_report_missing_and_completed_pods_with_their_finish_times() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let finished_at = "2026-10-09T19:00:00.250Z";
    let items: Vec<_> = [
        ("live", "Running", false),
        ("pending", "Pending", false),
        ("unknown", "Unknown", false),
        ("done", "Succeeded", true),
        ("crashed", "Failed", true),
        ("exited", "Running", true),
        ("replaced", "Running", false),
    ]
    .into_iter()
    .map(|(name, phase, exited)| pod(name, phase, exited.then_some(finished_at)))
    .collect();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fail_request = fail.clone();
    let routes = Router::new().route("/api/v1/namespaces/sandboxes/pods", get(move || {
        let fail = fail_request.clone();
        let items = items.clone();
        async move {
            if fail.load(std::sync::atomic::Ordering::SeqCst) {
                Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
            } else {
                Ok(Json(json!({"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"10"},"items":items})))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
        },
        false,
    );
    let spares: Vec<_> = [
        "live", "pending", "unknown", "done", "crashed", "exited", "missing", "replaced",
    ]
    .into_iter()
    .map(|name| SpareHandle {
        name: name.into(),
        resource_id: format!(
            "sandboxes/{name}/{}-uid",
            if name == "replaced" { "old" } else { name }
        ),
        route: String::new(),
        canonical_region: String::new(),
        control_route: String::new(),
        control_token: String::new(),
    })
    .collect();
    let stopped = cluster.stopped_spares(&spares).await?;
    let finished_at_ms =
        k8s_openapi::chrono::DateTime::parse_from_rfc3339(finished_at)?.timestamp_millis();
    assert_eq!(
        stopped,
        spares[3..]
            .iter()
            .zip([
                Some(finished_at_ms),
                Some(finished_at_ms),
                Some(finished_at_ms),
                None,
                None,
            ])
            .map(|(spare, stopped_at_ms)| StoppedSpare {
                resource_id: spare.resource_id.clone(),
                stopped_at_ms,
            })
            .collect::<Vec<_>>()
    );
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(cluster.stopped_spares(&spares).await.is_err());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn orphaned_completed_pods_are_released_once_their_stop_could_have_been_recorded()
-> Result<()> {
    use axum::{
        Json, Router,
        extract::{Path, Query},
        http::Method,
        routing::{delete, get},
    };
    let items = vec![
        pod("recent", "Succeeded", Some("2026-10-09T19:08:00Z")),
        pod("expired", "Succeeded", Some("2026-10-09T19:00:00Z")),
        pod("unrecorded", "Failed", None),
    ];
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let patched = calls.clone();
    let routes = Router::new()
        .route(
            "/api/v1/namespaces/sandboxes/pods",
            get(move |Query(query): Query<HashMap<String, String>>| async move {
                let phase = query["fieldSelector"].trim_start_matches("status.phase=");
                let items: Vec<_> = items
                    .into_iter()
                    .filter(|pod| pod["status"]["phase"] == phase)
                    .collect();
                Json(json!({"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"10"},"items":items}))
            }),
        )
        .route(
            "/api/v1/namespaces/sandboxes/pods/{name}",
            delete(move |method: Method, Path(name): Path<String>| async move {
                recorded.lock().unwrap().push((method.to_string(), name));
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            })
            .patch(
                move |method: Method, Path(name): Path<String>| async move {
                    let pod = pod(&name, "Succeeded", None);
                    patched.lock().unwrap().push((method.to_string(), name));
                    Json(pod)
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let now = k8s_openapi::chrono::DateTime::parse_from_rfc3339("2026-10-09T19:10:00Z")?;
    let result = reap_completed(
        &Api::namespaced(Client::try_from(config)?, "sandboxes"),
        now.timestamp_millis(),
    )
    .await;
    server.abort();
    result?;
    let calls = calls.lock().unwrap().clone();
    assert_eq!(
        calls
            .iter()
            .map(|(method, name)| (method.as_str(), name.as_str()))
            .collect::<Vec<_>>(),
        [
            ("PATCH", "unrecorded"),
            ("DELETE", "unrecorded"),
            ("PATCH", "expired"),
            ("DELETE", "expired"),
        ]
    );
    Ok(())
}

fn pod(name: &str, phase: &str, finished_at: Option<&str>) -> serde_json::Value {
    let mut pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":"sandboxes","uid":format!("{name}-uid"),"labels":{"app.kubernetes.io/managed-by":"terse"}},"status":{"phase":phase}});
    if let Some(finished_at) = finished_at {
        pod["status"]["containerStatuses"] = json!([{"name":"runtime","image":"runtime","imageID":"","ready":false,"restartCount":0,"state":{"terminated":{"exitCode":0,"finishedAt":finished_at}}}]);
    }
    pod
}

#[test]
fn temporary_storage_is_disk_backed_and_bounded() {
    let pod = base_pod(
        "actor",
        "image",
        &["us-west4-a".into()],
        &ResourceLimits {
            cpu_millis: 500,
            memory_mib: 143,
        },
    );
    let resources = &pod["spec"]["containers"][0]["resources"];
    assert_eq!(resources["requests"]["ephemeral-storage"], "1Gi");
    assert_eq!(resources["limits"]["ephemeral-storage"], "8Gi");
    for volume in pod["spec"]["volumes"].as_array().unwrap() {
        assert!(volume["emptyDir"]["medium"].is_null());
        assert!(volume["emptyDir"]["sizeLimit"].is_string());
    }
}
