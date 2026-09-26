use super::*;

#[tokio::test]
async fn subscribers_receive_the_claimed_route_and_entries_are_released() {
    let hints = RouteHints::default();
    let mut receiver = hints.subscribe("actor-a");
    hints.publish("actor-b", "https://other.test");
    hints.publish("actor-a", "https://host.test");
    receiver.changed().await.unwrap();
    assert_eq!(receiver.borrow().as_deref(), Some("https://host.test"));
    hints.release("actor-a", receiver);
    assert!(hints.is_empty());
}

#[test]
fn publishing_without_subscribers_keeps_nothing() {
    let hints = RouteHints::default();
    hints.publish("actor-a", "https://host.test");
    assert!(hints.is_empty());
}
