use super::*;

#[test]
fn accepts_a_single_safe_storage_component() {
    ActorStorageKey::new("tenant_1.session-123")
        .validate()
        .expect("valid actor storage key");
}

#[test]
fn rejects_ids_that_can_escape_or_reshape_storage_paths() {
    for id in [
        "",
        ".",
        "..",
        "../other",
        "nested/object",
        "windows\\path",
        "bad\0id",
    ] {
        ActorStorageKey::new(id)
            .validate()
            .expect_err("unsafe actor storage key");
    }
}
