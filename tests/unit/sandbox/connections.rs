use super::*;
use std::{collections::HashMap, net::Ipv4Addr, sync::Mutex};

use axum::{
    Router,
    extract::{ConnectInfo, State},
    http::{Uri, Version},
    routing::{get, post},
};
use hickory_resolver::{
    config::{NameServerConfig, ResolverConfig},
    net::runtime::TokioRuntimeProvider,
    proto::{
        op::{Message, MessageType},
        rr::{RData, Record, rdata::A},
    },
};

#[tokio::test]
async fn warmup_reuses_assignment_connection_and_caches_invocation_dns() -> Result<()> {
    let lookups = Arc::new(Mutex::new(HashMap::new()));
    let (resolver, dns) = dns_server(lookups.clone()).await?;
    let connections = HostConnections::with_resolver(Arc::new(DnsResolver(resolver)))?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let app = Router::new()
        .route("/healthz", get(observe))
        .route("/assign", post(observe))
        .route("/invoke", post(observe))
        .with_state(requests.clone());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let spare = spare(port);
    connections.prewarm(&spare).await;
    assert_eq!(lookups.lock().unwrap().get("invoke.test."), Some(&1));
    let client = connections.clone().client;
    for url in [
        format!("{}/assign", spare.control_route),
        format!("{}/invoke", spare.route),
    ] {
        assert_eq!(client.post(url).send().await?.text().await?, "ok");
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].0, "/healthz");
    assert_eq!(requests[1].0, "/assign");
    assert_eq!(
        requests[0].1, requests[1].1,
        "assignment must reuse the warm socket"
    );
    assert!(requests.iter().all(|request| request.2 == Version::HTTP_2));
    assert_eq!(lookups.lock().unwrap().get("invoke.test."), Some(&1));
    assert_eq!(lookups.lock().unwrap().get("control.test."), Some(&1));
    server.abort();
    dns.abort();
    Ok(())
}

#[tokio::test]
async fn failed_warmup_is_bounded_and_does_not_prevent_later_requests() -> Result<()> {
    let connections = HostConnections::with_resolver(Arc::new(StalledResolver))?;
    let spare = spare(1);
    let warming = connections.prewarm(&spare);
    tokio::time::timeout(Duration::from_secs(3), warming).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/healthz", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/healthz", get(|| async { "ok" })),
        )
        .await
        .unwrap();
    });
    assert_eq!(
        connections.client.get(url).send().await?.text().await?,
        "ok"
    );
    server.abort();
    Ok(())
}

type Observations = Arc<Mutex<Vec<(String, SocketAddr, Version)>>>;

async fn observe(
    State(requests): State<Observations>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    uri: Uri,
    version: Version,
) -> &'static str {
    requests
        .lock()
        .unwrap()
        .push((uri.path().into(), peer, version));
    "ok"
}

fn spare(port: u16) -> SpareHandle {
    SpareHandle {
        name: "spare".into(),
        resource_id: "sandbox".into(),
        route: format!("http://invoke.test:{port}"),
        control_route: format!("http://control.test:{port}"),
        control_token: "secret".into(),
        canonical_region: "north-america-west".into(),
    }
}

async fn dns_server(
    lookups: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<(TokioResolver, tokio::task::JoinHandle<()>)> {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let mut nameserver = NameServerConfig::udp(address.ip());
    nameserver.connections[0].port = address.port();
    let mut builder = TokioResolver::builder_with_config(
        ResolverConfig::from_name_servers(vec![nameserver]),
        TokioRuntimeProvider::default(),
    );
    builder.options_mut().ip_strategy = LookupIpStrategy::Ipv4Only;
    let resolver = builder.build()?;
    let task = tokio::spawn(async move {
        let mut buffer = [0; 4096];
        loop {
            let (length, peer) = socket.recv_from(&mut buffer).await.unwrap();
            let request = Message::from_vec(&buffer[..length]).unwrap();
            let query = request.queries[0].clone();
            *lookups
                .lock()
                .unwrap()
                .entry(query.name().to_string())
                .or_default() += 1;
            let mut response = Message::new(request.id, MessageType::Response, request.op_code);
            response.metadata.recursion_desired = true;
            response.metadata.recursion_available = true;
            response.add_query(query.clone());
            response.add_answer(Record::from_rdata(
                query.name().clone(),
                60,
                RData::A(A(Ipv4Addr::LOCALHOST)),
            ));
            socket
                .send_to(&response.to_vec().unwrap(), peer)
                .await
                .unwrap();
        }
    });
    Ok((resolver, task))
}

struct StalledResolver;

impl Resolve for StalledResolver {
    fn resolve(&self, _: Name) -> Resolving {
        Box::pin(std::future::pending())
    }
}
