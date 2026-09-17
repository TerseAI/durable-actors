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
        Self::with_credentials(
            bucket,
            google_cloud_auth::credentials::Builder::default().build()?,
        )
        .await
    }

    pub async fn with_credentials(
        bucket: &str,
        credentials: google_cloud_auth::credentials::Credentials,
    ) -> Result<Self> {
        anyhow::ensure!(
            !bucket.is_empty() && !bucket.contains('/'),
            "invalid storage bucket"
        );
        Ok(Self {
            bucket: format!("projects/_/buckets/{bucket}"),
            storage: Storage::builder()
                .with_credentials(credentials.clone())
                .build()
                .await?,
            control: StorageControl::builder()
                .with_credentials(credentials)
                .build()
                .await?,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Context;
    use axum::{
        Json, Router,
        body::Bytes,
        extract::{Path, Query},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use google_cloud_auth::credentials::anonymous;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn gcs_adapter_preserves_generation_conditions_and_pagination() -> Result<()> {
        let routes = Router::new()
            .route("/storage/v1/b/test-bucket/o/{*key}", get(read))
            .route("/google.storage.v2.Storage/ListObjects", post(list))
            .route("/upload/storage/v1/b/test-bucket/o", post(write));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let bucket = GcsBucket {
            bucket: "projects/_/buckets/test-bucket".into(),
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
        let key = "runtime/lease.json";
        let object = bucket.get(key).await.context("read object")?.unwrap();
        assert_eq!(object.generation, 42);
        assert_eq!(object.bytes, b"lease");
        assert!(bucket.get("missing").await?.is_none());
        assert!(bucket.get("forbidden").await.is_err());
        assert!(
            bucket
                .compare_and_swap(key, None, b"lease".to_vec())
                .await
                .context("create object")?
        );
        assert!(
            bucket
                .compare_and_swap(key, Some(object.generation), b"lease".to_vec())
                .await
                .context("replace object")?
        );
        assert!(
            !bucket
                .compare_and_swap(key, Some(41), b"lease".to_vec())
                .await?
        );
        assert!(
            bucket
                .compare_and_swap(key, Some(43), b"lease".to_vec())
                .await
                .is_err()
        );
        assert_eq!(
            bucket.list("runtime/").await.context("list objects")?,
            ["runtime/one.json", "runtime/two.json"]
        );
        server.abort();
        Ok(())
    }

    async fn read(Path(key): Path<String>) -> Response {
        match key.as_str() {
            "runtime/lease.json" => ([("x-goog-generation", "42")], "lease").into_response(),
            "missing" => error(StatusCode::NOT_FOUND),
            _ => error(StatusCode::FORBIDDEN),
        }
    }

    async fn write(Query(query): Query<HashMap<String, String>>, body: Bytes) -> Response {
        let body = String::from_utf8_lossy(&body);
        if query.get("uploadType").map(String::as_str) != Some("multipart")
            || query.get("name").map(String::as_str) != Some("runtime/lease.json")
            || !body.contains("lease")
        {
            return error(StatusCode::BAD_REQUEST);
        }
        match query.get("ifGenerationMatch").map(String::as_str) {
            Some("0" | "42") => Json(json!({"generation":"43"})).into_response(),
            Some("41") => error(StatusCode::PRECONDITION_FAILED),
            Some("43") => error(StatusCode::FORBIDDEN),
            _ => error(StatusCode::BAD_REQUEST),
        }
    }

    async fn list(request: axum::extract::Request) -> Response {
        tonic::server::Grpc::new(tonic_prost::ProstCodec::<ListResponse, ListRequest>::default())
            .unary(ListService, request)
            .await
            .map(axum::body::Body::new)
    }

    fn error(status: StatusCode) -> Response {
        (
            status,
            Json(json!({"error":{"code":status.as_u16(),"message":status.as_str()}})),
        )
            .into_response()
    }

    struct ListService;

    impl tonic::server::UnaryService<ListRequest> for ListService {
        type Response = ListResponse;
        type Future = std::future::Ready<Result<tonic::Response<ListResponse>, tonic::Status>>;

        fn call(&mut self, request: tonic::Request<ListRequest>) -> Self::Future {
            let request = request.into_inner();
            let result = if request.parent != "projects/_/buckets/test-bucket"
                || request.prefix != "runtime/"
            {
                Err(tonic::Status::invalid_argument(
                    "unexpected bucket or prefix",
                ))
            } else {
                match request.page_token.as_str() {
                    "" => Ok(("runtime/one.json", "next")),
                    "next" => Ok(("runtime/two.json", "")),
                    _ => Err(tonic::Status::invalid_argument("unexpected page token")),
                }
            };
            std::future::ready(result.map(|(name, token)| {
                tonic::Response::new(ListResponse {
                    objects: vec![ListedObject { name: name.into() }],
                    next_page_token: token.into(),
                })
            }))
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ListRequest {
        #[prost(string, tag = "1")]
        parent: String,
        #[prost(string, tag = "3")]
        page_token: String,
        #[prost(string, tag = "6")]
        prefix: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ListResponse {
        #[prost(message, repeated, tag = "1")]
        objects: Vec<ListedObject>,
        #[prost(string, tag = "3")]
        next_page_token: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ListedObject {
        #[prost(string, tag = "1")]
        name: String,
    }
}
