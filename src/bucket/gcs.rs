use anyhow::Result;
use async_trait::async_trait;
use google_cloud_storage::client::{Storage, StorageControl};

use super::{Bucket, BucketObject};

pub struct GcsBucket {
    bucket: String,
    storage: Storage,
    control: StorageControl,
}

impl GcsBucket {
    pub async fn new(bucket: &str) -> Result<Self> {
        anyhow::ensure!(
            !bucket.is_empty() && !bucket.contains('/'),
            "invalid coordination bucket"
        );
        Ok(Self {
            bucket: format!("projects/_/buckets/{bucket}"),
            storage: Storage::builder().build().await?,
            control: StorageControl::builder().build().await?,
        })
    }
}

#[async_trait]
impl Bucket for GcsBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        let mut response = match self.storage.read_object(&self.bucket, key).send().await {
            Ok(response) => response,
            Err(error)
                if error.http_status_code() == Some(404)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let generation = response.object().generation;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Some(BucketObject { generation, bytes }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        match self
            .storage
            .write_object(&self.bucket, key, bytes::Bytes::from(bytes))
            .set_if_generation_match(generation.unwrap_or(0))
            .send_unbuffered()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.http_status_code() == Some(412)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "FAILED_PRECONDITION") =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = String::new();
        loop {
            let response = self
                .control
                .list_objects()
                .set_parent(&self.bucket)
                .set_prefix(prefix)
                .set_page_token(&token)
                .send()
                .await?;
            keys.extend(response.objects.into_iter().map(|object| object.name));
            token = response.next_page_token;
            if token.is_empty() {
                return Ok(keys);
            }
        }
    }
}
