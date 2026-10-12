use super::{proto::*, *};
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, ResourceExt};
use terse_substrate::{Client, ConnectionOptions, FileCredentials};
use tonic::Code;

pub(super) struct GrpcApi {
    client: Client,
    secrets: Api<Secret>,
}

impl GrpcApi {
    pub async fn connect(config: &SubstrateConfig) -> Result<Self> {
        let client = Client::connect(
            ConnectionOptions::new(&config.endpoint, &config.trust_bundle),
            Arc::new(FileCredentials::new(&config.token_file)),
        )
        .await?;
        client
            .ensure_atespace(Atespace {
                metadata: Some(ResourceMetadata {
                    name: config.atespace.clone(),
                    ..Default::default()
                }),
            })
            .await?;
        Ok(Self {
            client,
            secrets: Api::namespaced(
                kube::Client::try_default().await?,
                &config.secrets_namespace,
            ),
        })
    }

    async fn delete_tag(&self, tag: Tag) -> Result<()> {
        let meta = tag.metadata.context("tag identity missing")?;
        self.client
            .delete_tag(DeleteTagRequest {
                tag: Some(reference(&meta.atespace, &meta.name)),
                options: Some(DeleteOptions {
                    uid: meta.uid,
                    version: meta.version,
                }),
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SubstrateApi for GrpcApi {
    async fn template(&self, template: ActorTemplate) -> Result<ActorTemplate> {
        self.client.ensure_template(template).await
    }

    async fn tag(&self, target: ObjectRef) -> Result<Option<Tag>> {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let tag = match self.client.get_tag(target.clone()).await {
                    Ok(tag) => tag,
                    Err(error) if is_status(&error, Code::NotFound) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if tag.status.as_ref().is_some_and(|s| s.snapshot.is_some()) {
                    return Ok(Some(tag));
                }
                let meta = tag.metadata.as_ref().context("tag identity missing")?;
                let stale = meta.create_time.as_ref().is_some_and(|t| {
                    SystemClock
                        .now_ms()
                        .is_ok_and(|now| (now / 1000).saturating_sub(t.seconds as u64) > 600)
                });
                if stale {
                    self.delete_tag(tag).await?;
                    return Ok(None);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("timed out waiting for code snapshot")?
    }

    async fn suspend(&self, actor: ObjectRef) -> Result<()> {
        self.client.suspend_actor(actor).await?;
        Ok(())
    }

    async fn create_tag(&self, tag: Tag) -> Result<()> {
        let meta = tag.metadata.as_ref().context("tag identity missing")?;
        let target = reference(&meta.atespace, &meta.name);
        let source = tag.source_actor.clone();
        match self.client.create_tag(tag).await {
            Ok(_) => {}
            Err(error) if is_status(&error, Code::AlreadyExists) => {}
            Err(error) => {
                if let Ok(failed) = self.client.get_tag(target.clone()).await
                    && failed.source_actor == source
                    && failed.status.as_ref().is_none_or(|s| s.snapshot.is_none())
                    && let Err(cleanup) = self.delete_tag(failed).await
                {
                    tracing::warn!(%cleanup, "failed to collect incomplete code snapshot");
                }
                return Err(error);
            }
        }
        self.tag(target)
            .await?
            .context("created code snapshot is missing")?;
        Ok(())
    }

    async fn create(&self, actor: Actor) -> Result<Actor> {
        self.client.create_actor(actor).await
    }

    async fn egress(&self, actor: ObjectRef, rules: Vec<EgressRule>) -> Result<()> {
        let policy = EgressPolicy {
            metadata: Some(ResourceMetadata {
                atespace: actor.atespace.clone(),
                name: "default".into(),
                ..Default::default()
            }),
            rules,
        };
        self.client
            .create_actor_egress_policy(CreateActorEgressPolicyRequest {
                actor: Some(actor),
                egress_policy: Some(policy),
            })
            .await?;
        Ok(())
    }

    async fn resume(&self, actor: ObjectRef) -> Result<()> {
        self.client.resume_actor(actor).await?;
        Ok(())
    }

    async fn delete(&self, actor: Actor, version: Option<i64>) -> Result<()> {
        let meta = actor.metadata.context("actor identity missing")?;
        let request = DeleteActorRequest {
            actor: Some(reference(&meta.atespace, &meta.name)),
            any_state: true,
            options: Some(DeleteOptions {
                uid: meta.uid,
                version: version.unwrap_or(0),
            }),
        };
        match self.client.delete_actor(request).await {
            Ok(_) => Ok(()),
            Err(error) if is_status(&error, Code::NotFound) => Ok(()),
            Err(error) if version.is_some() && is_status(&error, Code::Aborted) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn workers(&self) -> Result<Vec<Worker>> {
        self.client.list_workers().await
    }

    async fn actors(&self, atespace: &str) -> Result<Vec<Actor>> {
        self.client.list_actors(atespace).await
    }

    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>> {
        let loaded =
            futures_util::future::try_join_all(names.iter().map(|name| self.secrets.get(name)))
                .await?;
        let mut environment = HashMap::new();
        for secret in loaded {
            ensure!(
                secret
                    .labels()
                    .get("terse.ai/customer-secret")
                    .is_some_and(|v| v == "true"),
                "secret is not marked for customer code"
            );
            for (name, value) in secret.data.unwrap_or_default() {
                ensure!(
                    !environment.contains_key(&name),
                    "duplicate customer environment variable {name}"
                );
                environment.insert(
                    name,
                    String::from_utf8(value.0).context("customer secret must be UTF-8")?,
                );
            }
        }
        Ok(environment)
    }
}

fn is_status(error: &anyhow::Error, code: Code) -> bool {
    error
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == code)
}
