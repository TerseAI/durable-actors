use super::*;
use serde_json::json;

#[test]
fn session_permissions_are_intersected_per_actor_with_published_methods() -> Result<()> {
    let session: ActorSession = serde_json::from_value(
        json!({"iss":"runtime", "aud":"sessions", "sub":"credential", "jti":"session",
        "projectId":"project", "scope":"actor:session", "iat":1000,"nbf":1000,"exp":1060,
        "permissions":[{"actorName":"Counter","actorId":"one","methods":["read","hidden"]},
            {"actorName":"Counter","actorId":"two","methods":["write"]}]}),
    )?;
    session.validate(1000)?;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let grant = session
        .clone()
        .invocation(&actor, vec!["read".into(), "write".into()])?;
    assert_eq!(grant.methods, vec!["read"]);
    assert!(!session.contains(&ActorKey {
        actor_id: "three".into(),
        ..actor.clone()
    }));
    assert!(!session.contains(&ActorKey {
        project_id: "other".into(),
        ..actor
    }));
    assert!(session.validate(1060).is_err());
    Ok(())
}
