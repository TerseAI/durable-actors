use super::*;

fn source() -> SourceArchive {
    SourceArchive {
        sha256: "a".repeat(64),
        entrypoint: "src/actor.ts".into(),
        object: Some(SourceObject {
            bucket: "source-bucket".into(),
            name: "sdk-deploys/team/source.zip".into(),
            generation: "42".into(),
        }),
    }
}

#[test]
fn source_identity_rejects_unpinned_objects_and_escaping_entrypoints() {
    let valid = source();
    assert!(valid.validate().is_ok());
    for entrypoint in [
        "../actor.ts",
        "/actor.ts",
        "src/../actor.ts",
        "src/actor.txt",
    ] {
        let mut invalid = valid.clone();
        invalid.entrypoint = entrypoint.into();
        assert!(invalid.validate().is_err(), "{entrypoint}");
    }
    let mut invalid = valid.clone();
    invalid.object.as_mut().unwrap().generation = "0".into();
    assert!(invalid.validate().is_err());
    invalid = valid;
    invalid.sha256 = "latest".into();
    assert!(invalid.validate().is_err());
}

#[test]
fn compiled_cache_identity_is_project_and_runtime_scoped_but_ignores_upload_location() {
    let first = source();
    let mut second = first.clone();
    second.object.as_mut().unwrap().name = "sdk-deploys/team/retry.zip".into();
    assert_eq!(
        first.cache_key("project-a", "runtime-a"),
        second.cache_key("project-a", "runtime-a")
    );
    assert_ne!(
        first.cache_key("project-a", "runtime-a"),
        first.cache_key("project-b", "runtime-a")
    );
    assert_ne!(
        first.cache_key("project-a", "runtime-a"),
        first.cache_key("project-a", "runtime-b")
    );
    second.entrypoint = "src/other.ts".into();
    assert_ne!(
        first.cache_key("project-a", "runtime-a"),
        second.cache_key("project-a", "runtime-a")
    );
    second = first.clone();
    second.object = None;
    assert_eq!(
        first.cache_key("project-a", "runtime-a"),
        second.cache_key("project-a", "runtime-a")
    );
}
