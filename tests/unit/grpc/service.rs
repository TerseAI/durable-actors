use super::*;
use crate::{
    actor::ActorKey,
    control_plane::{ActorInvocationCapability, ActorPrincipal},
};

#[test]
fn direct_capability_is_bound_to_the_actor_host_session_and_epoch() {
    let actor = ActorKey {
        actor_type: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let host_id = HostId::new("host.v3.revision-1.host-1");
    let principal = ActorPrincipal {
        actor: actor.clone(),
        host_id: host_id.clone(),
        session_id: "00000000-0000-4000-8000-000000000001".into(),
        region: "north-america-east".into(),
        code_revision: Some("revision-1".into()),
        invocation: Some(ActorInvocationCapability {
            actor: actor.clone(),
            host_id: host_id.clone(),
            owner_epoch: 3,
        }),
    };

    assert!(validate_activation(&principal, &host_id, &principal.session_id).is_err());
    let mut activation = principal.clone();
    activation.invocation = None;
    assert!(validate_activation(&activation, &host_id, &activation.session_id).is_ok());
    assert!(validate_activation(&activation, &host_id, "another-session").is_err());
    assert!(
        validate_activation(
            &activation,
            &HostId::new("another-host"),
            &activation.session_id
        )
        .is_err()
    );

    assert!(validate_host_request(&principal, &host_id, &actor, 3).is_ok());
    assert!(validate_host_request(&principal, &host_id, &actor, 4).is_err());
    assert!(
        validate_host_request(
            &principal,
            &host_id,
            &ActorKey {
                actor_id: "other".into(),
                ..actor
            },
            3
        )
        .is_err()
    );
}
