use super::*;
use crate::bucket::{Durability, FileBucket, ReplicaPlacement};
use tokio::task::JoinSet;

pub(crate) struct ReplicaCluster {
    pub config: PersistenceConfig,
    pub access: Access,
    servers: Vec<Arc<ReplicaServer>>,
    tasks: JoinSet<()>,
    _directory: tempfile::TempDir,
}

impl ReplicaCluster {
    pub async fn start(count: usize) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let archive = Archive(Arc::new(FileBucket::new(directory.path().join("archive"))?));
        let access = Access::new(uuid::Uuid::new_v4().to_string())?;
        let mut listeners = Vec::new();
        let mut placements = Vec::new();
        for i in 0..count {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            placements.push(ReplicaPlacement {
                id: format!("replica-{i}"),
                address: format!("http://{}", listener.local_addr()?),
                zone: "us-west4-a".into(),
            });
            listeners.push(listener);
        }
        let mut tasks = JoinSet::new();
        let mut servers = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let peers = placements
                .iter()
                .filter(|p| p.id != placements[i].id)
                .map(|p| ReplicaClient::new(p.clone(), access.admin().into()))
                .collect::<Result<_>>()?;
            let server = Arc::new(ReplicaServer {
                id: placements[i].id.clone(),
                disk: ReplicaDisk::open(directory.path().join(format!("{i}.sqlite"))).await?,
                archive: archive.clone(),
                access: access.clone(),
                peers,
                primary: i == 0,
                clock: Arc::new(SystemClock),
            });
            register_disk(&archive, &server.disk, &server.id).await?;
            let app = routes(server.clone());
            tasks.spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let uploader = server.clone();
            tasks.spawn(async move {
                uploader.upload_loop(CancellationToken::new()).await;
            });
            servers.push(server);
        }
        Ok(Self {
            config: PersistenceConfig::Replicated {
                replicas: placements,
                durability: Durability::Zonal,
            },
            access,
            servers,
            tasks,
            _directory: directory,
        })
    }

    pub async fn wait_archived(&self, prefixes: &[String]) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut pending = false;
                for server in &self.servers {
                    for prefix in prefixes {
                        let active = server.disk.batch(prefix, BATCH_BYTES).await?.is_some();
                        pending |= active;
                        if !active {
                            ensure!(
                                server.disk.latest(prefix).await?.is_none(),
                                "sealed epoch retained archived payload"
                            );
                        }
                    }
                }
                if !pending {
                    return anyhow::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;
        Ok(())
    }
}

impl Drop for ReplicaCluster {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}
