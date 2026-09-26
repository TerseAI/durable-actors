use super::*;

#[tokio::test]
async fn proxy_exits_after_idle_but_keeps_active_sessions_alive() {
    let activity = Arc::new(Activity::default());
    let active = activity.enter().unwrap();
    let idle = activity.wait_until_idle(Duration::from_millis(20));
    tokio::pin!(idle);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut idle)
            .await
            .is_err()
    );
    drop(active);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut idle)
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_millis(60), &mut idle)
        .await
        .unwrap();
    assert!(activity.enter().is_none());
}
