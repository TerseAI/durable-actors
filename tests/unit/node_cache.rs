use super::*;
use crate::{
    actor::ActorKey,
    control_plane::{ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, HostAuthorization},
    host::HostId,
};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use axum::serve::ListenerExt;
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::TryStreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::task::AbortOnDropHandle;

#[tokio::test]
async fn hosts_share_code_through_the_node_cache() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let code = tempfile::NamedTempFile::new()?;
    std::fs::write(code.path(), b"export default 1")?;
    let digest = sha256(b"export default 1");
    let host = cache.client(&host_token(&issuer, "one")?)?;

    assert!(host.code(&digest).await?.is_none());
    host.store_code(&digest, code.path()).await?;
    let chunks: Vec<bytes::Bytes> = host.code(&digest).await?.unwrap().try_collect().await?;

    assert_eq!(chunks.concat(), b"export default 1");
    Ok(())
}

#[tokio::test]
async fn node_cache_rejects_unauthenticated_hosts_and_mismatched_code() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let code = tempfile::NamedTempFile::new()?;
    std::fs::write(code.path(), b"tampered")?;
    let digest = sha256(b"original");
    let host = cache.client(&host_token(&issuer, "one")?)?;

    assert!(cache.client("forged")?.code(&digest).await.is_err());
    assert!(host.store_code(&digest, code.path()).await.is_err());
    assert!(host.code(&digest).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn saved_databases_are_only_visible_to_hosts_of_the_same_actor() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let database = tempfile::NamedTempFile::new()?;
    std::fs::write(database.path(), b"sqlite bytes")?;
    let snapshot = sha256(b"snapshot");
    let owner = cache.client(&host_token(&issuer, "one")?)?;

    owner.store_database(&snapshot, database.path()).await?;

    let restored = owner.database(&snapshot).await?.unwrap().receive().await?;
    assert_eq!(std::fs::read(restored.path())?, b"sqlite bytes");
    let other = cache.client(&host_token(&issuer, "two")?)?;
    assert!(other.database(&snapshot).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn saved_snapshots_must_match_their_digest_and_stay_with_their_actor() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let digest = sha256(b"snapshot");
    let owner = cache.client(&host_token(&issuer, "one")?)?;

    assert!(owner.snapshot(&digest).await?.is_none());
    assert!(
        owner
            .store_snapshot(&digest, "tampered".into())
            .await
            .is_err()
    );
    owner.store_snapshot(&digest, "snapshot".into()).await?;

    assert_eq!(owner.snapshot(&digest).await?.unwrap(), "snapshot");
    let other = cache.client(&host_token(&issuer, "two")?)?;
    assert!(other.snapshot(&digest).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn warm_connections_serve_every_activation_read_at_once() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let connection = NodeCacheConnection::new(&cache.url)?;
    connection.preconnect().await;
    connection.preconnect().await;
    let warmed = cache.connections.load(Ordering::SeqCst);
    let host = connection.authorize(HostAuthorization::fixed(&host_token(&issuer, "one")?)?);
    let digest = sha256(b"snapshot");

    let (code, database, snapshot) = tokio::join!(
        host.code(&digest),
        host.database(&digest),
        host.snapshot(&digest)
    );

    assert!(code?.is_none() && database?.is_none() && snapshot?.is_none());
    assert_eq!(cache.connections.load(Ordering::SeqCst), warmed);
    Ok(())
}

struct NodeCache {
    url: String,
    connections: Arc<AtomicUsize>,
    _directory: tempfile::TempDir,
    _server: AbortOnDropHandle<std::io::Result<()>>,
}

impl NodeCache {
    async fn start(issuer: &ActorJwtIssuer) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(store::DiskStore::open(directory.path().into(), 1 << 20).await?);
        let tokens = keys::HostTokens::fixed(ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(1800),
        )?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let listener = listener.tap_io(move |_| {
            accepted.fetch_add(1, Ordering::SeqCst);
        });
        let routes = server::router(store, tokens);
        Ok(Self {
            url,
            connections,
            _directory: directory,
            _server: AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(listener, routes).await
            })),
        })
    }

    fn client(&self, token: &str) -> Result<NodeCacheClient> {
        Ok(NodeCacheConnection::new(&self.url)?.authorize(HostAuthorization::fixed(token)?))
    }
}

fn issuer() -> Result<ActorJwtIssuer> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(1800),
    )
}

fn host_token(issuer: &ActorJwtIssuer, actor_id: &str) -> Result<String> {
    Ok(issuer
        .issue_host(
            "org-test",
            &HostId::new("host.v3.r1.one"),
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "us-west",
            &ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: actor_id.into(),
            },
        )?
        .token)
}

fn sha256(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}
