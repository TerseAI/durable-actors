use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const KEY: [u8; 32] = [7; 32];
const OWNER: &str = "projects/_/buckets/authority/objects/owner.json";

fn session_key_with(type_url: &str, output_prefix_type: i32) -> String {
    let keyset = Keyset {
        primary_key_id: 42,
        key: vec![KeysetKey {
            key_data: Some(KeyData {
                type_url: type_url.into(),
                value: AesGcmKey {
                    version: 0,
                    key_value: KEY.to_vec(),
                }
                .encode_to_vec(),
                key_material_type: 1,
            }),
            status: KEY_STATUS_ENABLED,
            key_id: 42,
            output_prefix_type,
        }],
    };
    STANDARD.encode(keyset.encode_to_vec())
}

/// Hands out intermediaries with the given lifetimes in order, repeating the last one.
struct Exchanges {
    calls: Arc<AtomicUsize>,
    lifetimes: Vec<Duration>,
}

#[async_trait]
impl IntermediaryExchange for Exchanges {
    async fn exchange(&self) -> Result<IntermediaryToken> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let lifetime = self.lifetimes[call.min(self.lifetimes.len() - 1)];
        Ok(IntermediaryToken {
            access_token: format!("intermediary-{call}"),
            expires_at_ms: SystemClock.now_ms()? + lifetime.as_millis() as u64,
            session_key: session_key_with(AES_GCM_KEY_TYPE, 1),
        })
    }
}

fn tokens(lifetimes: &[Duration]) -> (Arc<ClientSideTokens>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let tokens = ClientSideTokens::new(Box::new(Exchanges {
        calls: calls.clone(),
        lifetimes: lifetimes.to_vec(),
    }));
    (Arc::new(tokens), calls)
}

fn rules() -> Vec<Rule> {
    vec![
        Rule {
            bucket: "authority".into(),
            roles: &["storage.objectUser"],
            terms: vec![Term::Equals(OWNER.into())],
        },
        Rule {
            bucket: "rapid".into(),
            roles: &["storage.objectUser"],
            terms: vec![
                Term::StartsWith("projects/_/buckets/rapid/objects/logs-1".into()),
                Term::ListPrefix("logs-1".into()),
            ],
        },
    ]
}

const HOUR: Duration = Duration::from_secs(3600);

fn decrypt(token: &StorageToken) -> (String, ClientSideAccessBoundary) {
    let (intermediary, encrypted) = token.access_token.rsplit_once('.').unwrap();
    let raw = URL_SAFE_NO_PAD.decode(encrypted).unwrap();
    assert_eq!(&raw[..5], &[1, 0, 0, 0, 42], "Tink output prefix");
    let mut sealed = raw[5 + NONCE_LEN..].to_vec();
    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &KEY).unwrap());
    let plain = key
        .open_in_place(
            Nonce::try_assume_unique_for_key(&raw[5..5 + NONCE_LEN]).unwrap(),
            Aad::empty(),
            &mut sealed,
        )
        .unwrap();
    (
        intermediary.to_owned(),
        ClientSideAccessBoundary::decode(&*plain).unwrap(),
    )
}

fn call(expr: &Expr) -> &Call {
    expr.call_expr.as_ref().expect("call expression")
}

fn string(expr: &Expr) -> &str {
    expr.const_expr
        .as_ref()
        .and_then(|constant| constant.string_value.as_deref())
        .expect("string constant")
}

fn is_resource_name(expr: &Expr) -> bool {
    let select = expr.select_expr.as_ref().unwrap();
    select.field == "name"
        && select
            .operand
            .as_ref()
            .unwrap()
            .ident_expr
            .as_ref()
            .unwrap()
            .name
            == "resource"
}

fn ids(expr: &Expr, out: &mut Vec<i64>) {
    out.push(expr.id);
    if let Some(select) = &expr.select_expr {
        ids(select.operand.as_ref().unwrap(), out);
    }
    if let Some(call) = &expr.call_expr {
        if let Some(target) = &call.target {
            ids(target, out);
        }
        call.args.iter().for_each(|arg| ids(arg, out));
    }
}

#[tokio::test]
async fn tokens_carry_the_boundary_encrypted_under_the_session_key() -> Result<()> {
    let (tokens, _) = tokens(&[HOUR]);
    let token = tokens.issue(&rules()).await?;
    let (intermediary, boundary) = decrypt(&token);
    assert_eq!(intermediary, "intermediary-0");
    assert!(token.expires_at_ms > SystemClock.now_ms()? + 59 * 60 * 1000);

    let [owner, logs] = boundary.access_boundary_rules.as_slice() else {
        panic!("two rules expected");
    };
    assert_eq!(
        owner.available_resource,
        "//storage.googleapis.com/projects/_/buckets/authority"
    );
    assert_eq!(
        owner.available_permissions,
        ["inRole:roles/storage.objectUser"]
    );
    let equals = call(owner.compiled_availability_condition.as_ref().unwrap());
    assert_eq!(equals.function, "_==_");
    assert!(equals.target.is_none());
    assert!(is_resource_name(&equals.args[0]));
    assert_eq!(string(&equals.args[1]), OWNER);

    let condition = logs.compiled_availability_condition.as_ref().unwrap();
    let or = call(condition);
    assert_eq!(or.function, "_||_");
    let starts_with = call(&or.args[0]);
    assert_eq!(starts_with.function, "startsWith");
    assert!(is_resource_name(starts_with.target.as_ref().unwrap()));
    assert_eq!(
        string(&starts_with.args[0]),
        "projects/_/buckets/rapid/objects/logs-1"
    );
    let list = call(&or.args[1]);
    assert_eq!(list.function, "startsWith");
    assert_eq!(string(&list.args[0]), "logs-1");
    let lookup = call(list.target.as_ref().unwrap());
    assert_eq!(lookup.function, "getAttribute");
    assert_eq!(
        lookup
            .target
            .as_ref()
            .unwrap()
            .ident_expr
            .as_ref()
            .unwrap()
            .name,
        "api"
    );
    assert_eq!(string(&lookup.args[0]), LIST_PREFIX_ATTRIBUTE);
    assert_eq!(string(&lookup.args[1]), "");

    let mut seen = Vec::new();
    ids(condition, &mut seen);
    let count = seen.len();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), count, "expression ids are unique");

    let again = tokens.issue(&rules()).await?;
    assert_ne!(
        again.access_token, token.access_token,
        "each token gets its own nonce"
    );
    assert_eq!(decrypt(&again).1, boundary);
    Ok(())
}

#[tokio::test]
async fn concurrent_launches_share_one_intermediary_exchange() -> Result<()> {
    let (tokens, calls) = tokens(&[HOUR]);
    let issued = futures_util::future::join_all((0..50).map(|_| {
        let tokens = tokens.clone();
        async move { tokens.issue(&rules()).await }
    }))
    .await;
    for token in issued {
        assert!(token?.access_token.starts_with("intermediary-0."));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn an_expiring_intermediary_is_replaced_without_blocking_launches() -> Result<()> {
    let (tokens, calls) = tokens(&[Duration::from_secs(180), HOUR]);
    let first = tokens.issue(&rules()).await?;
    assert!(first.access_token.starts_with("intermediary-0."));
    let deadline = Instant::now() + Duration::from_secs(5);
    let replacement = loop {
        let token = tokens.issue(&rules()).await?;
        if token.access_token.starts_with("intermediary-1.") {
            break token;
        }
        assert!(Instant::now() < deadline, "background refresh never landed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(replacement.expires_at_ms > SystemClock.now_ms()? + 59 * 60 * 1000);
    for _ in 0..20 {
        tokens.issue(&rules()).await?;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn intermediaries_too_close_to_expiry_are_refused() {
    let (tokens, _) = tokens(&[Duration::from_secs(60)]);
    let error = tokens.issue(&rules()).await.unwrap_err();
    assert!(error.to_string().contains("too short"), "{error:#}");
}

#[test]
fn session_keys_must_be_tink_aes_gcm() {
    let (_, prefix) = session_key(&session_key_with(AES_GCM_KEY_TYPE, 3)).unwrap();
    assert!(prefix.is_empty(), "RAW keys add no prefix");
    let (_, prefix) = session_key(&session_key_with(AES_GCM_KEY_TYPE, 2)).unwrap();
    assert_eq!(prefix, [0, 0, 0, 0, 42]);
    assert!(
        session_key(&session_key_with(
            "type.googleapis.com/google.crypto.tink.AesCtrHmacAeadKey",
            1
        ))
        .is_err()
    );
    assert!(session_key("not base64!").is_err());
}

#[test]
fn rules_without_conditions_are_refused() {
    let unconditional = Rule {
        bucket: "authority".into(),
        roles: &["storage.objectUser"],
        terms: Vec::new(),
    };
    assert!(restrictions(&[unconditional]).is_err());
}

/// Bytes from the encoder whose tokens staging GCS accepted on 2026-10-19 (JSON and Rapid gRPC,
/// every allow and deny matching server-side boundaries). A change here needs a new live check.
#[test]
fn restrictions_match_the_encoding_verified_against_gcs() {
    let rules = [
        Rule {
            bucket: "authority".into(),
            roles: &["storage.objectUser"],
            terms: vec![Term::Equals(
                "projects/_/buckets/authority/objects/owner.json".into(),
            )],
        },
        Rule {
            bucket: "rapid".into(),
            roles: &["storage.objectUser"],
            terms: vec![
                Term::StartsWith("projects/_/buckets/rapid/objects/logs-1".into()),
                Term::ListPrefix("logs-1".into()),
            ],
        },
        Rule {
            bucket: "code".into(),
            roles: &["storage.objectViewer"],
            terms: vec![Term::StartsWith(
                "projects/_/buckets/code/objects/artifacts/x/".into(),
            )],
        },
    ];
    let hex: String = restrictions(&rules)
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(
        hex,
        "0ab7010a352f2f73746f726167652e676f6f676c65617069732e636f6d2f70726f6a656374732f5f2f6275636b6574732f617574686f72697479121f696e526f6c653a726f6c65732f73746f726167652e6f626a65637455736572225d1004325912045f3d3d5f1a1a10022a160a0e1001220a0a087265736f7572636512046e616d651a3510031a31322f70726f6a656374732f5f2f6275636b6574732f617574686f726974792f6f626a656374732f6f776e65722e6a736f6e0ab5020a312f2f73746f726167652e676f6f676c65617069732e636f6d2f70726f6a656374732f5f2f6275636b6574732f7261706964121f696e526f6c653a726f6c65732f73746f726167652e6f626a6563745573657222de01100b32d90112045f7c7c5f1a5b100432570a1a10022a160a0e1001220a0a087265736f7572636512046e616d65120a737461727473576974681a2d10031a29322770726f6a656374732f5f2f6275636b6574732f72617069642f6f626a656374732f6c6f67732d311a74100a32700a54100832500a09100522050a03617069120c6765744174747269627574651a2d10061a29322773746f726167652e676f6f676c65617069732e636f6d2f6f626a6563744c6973745072656669781a0610071a023200120a737461727473576974681a0c10091a0832066c6f67732d310ab7010a302f2f73746f726167652e676f6f676c65617069732e636f6d2f70726f6a656374732f5f2f6275636b6574732f636f64651221696e526f6c653a726f6c65732f73746f726167652e6f626a65637456696577657222601004325c0a1a10022a160a0e1001220a0a087265736f7572636512046e616d65120a737461727473576974681a3210031a2e322c70726f6a656374732f5f2f6275636b6574732f636f64652f6f626a656374732f6172746966616374732f782f"
    );
}
