use super::*;
use aws_lc_rs::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::json;

#[test]
fn project_grants_are_bound_to_project_purpose_and_absolute_expiry() -> Result<()> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    let pair = Ed25519KeyPair::from_pkcs8(key.as_ref())?;
    let keys = json!({"keys":[{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"terse-1","x":URL_SAFE_NO_PAD.encode(pair.public_key().as_ref())}]}).to_string();
    let verifier = ProjectGrantVerifier::new(&keys, "terse", "actor-discovery")?;
    let now = unix_seconds()?;
    let claims = json!({"iss":"terse","aud":"actor-discovery","sub":"credential-1","jti":"grant-1","projectId":"project-a","scope":"actor:resolve","iat":now,"nbf":now,"exp":now+60});
    let sign = |claims: &serde_json::Value| {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("terse-1".into());
        encode(&header, claims, &EncodingKey::from_ed_der(key.as_ref())).unwrap()
    };
    let grant = verifier.authenticate(&format!("Bearer {}", sign(&claims)), "project-a")?;
    assert_eq!(grant.subject, "credential-1");
    assert_eq!(grant.expires_at, now + 60);
    assert!(
        verifier
            .authenticate(&format!("Bearer {}", sign(&claims)), "project-b")
            .is_err()
    );
    for (field, value) in [
        ("exp", json!(now)),
        ("exp", json!(now + 61)),
        ("scope", json!("actor:authority")),
        ("aud", json!("admin")),
        ("iss", json!("attacker")),
        ("sub", json!("")),
        ("jti", json!("")),
        ("iat", json!(now + 10)),
    ] {
        let mut invalid = claims.clone();
        invalid[field] = value;
        assert!(
            verifier
                .authenticate(&format!("Bearer {}", sign(&invalid)), "project-a")
                .is_err(),
            "accepted {field}"
        );
    }
    Ok(())
}
