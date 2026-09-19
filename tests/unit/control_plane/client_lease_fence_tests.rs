use super::*;

#[test]
fn an_early_request_does_not_prevent_initial_lease_confirmation() -> Result<()> {
    let now = Instant::now();
    let mut fence = LeaseFence::default();
    assert!(fence.check(now).is_err());
    fence.confirm(now, Duration::from_secs(30), now + Duration::from_secs(1))?;
    fence.check(now + Duration::from_secs(2))
}

#[test]
fn slow_renewal_cannot_revive_an_expired_process() -> Result<()> {
    let start = Instant::now();
    let mut fence = LeaseFence::default();
    fence.confirm(
        start,
        Duration::from_secs(30),
        start + Duration::from_secs(1),
    )?;
    fence.check(start + Duration::from_secs(24))?;
    assert!(
        fence
            .confirm(
                start + Duration::from_secs(20),
                Duration::from_secs(30),
                start + Duration::from_secs(26)
            )
            .is_err()
    );
    assert!(fence.begin(start + Duration::from_secs(27)).is_err());
    Ok(())
}

#[test]
fn response_gate_expires_from_request_start_even_without_a_timer() -> Result<()> {
    let start = Instant::now();
    let mut fence = LeaseFence::default();
    fence.confirm(
        start,
        Duration::from_secs(30),
        start + Duration::from_secs(10),
    )?;
    assert!(fence.check(start + Duration::from_secs(25)).is_err());
    Ok(())
}
