use crate::sandbox::pool::sizing::{History, Policy, Target};

fn policy() -> Policy {
    Policy {
        minimum: 5,
        maximum: 32,
        shrink_after_seconds: 300,
    }
}

fn target(value: u32) -> Target {
    Target {
        size: value,
        shrink_at_ms: None,
    }
}

#[test]
fn quiet_pools_keep_the_configured_floor_and_zero_disables_warming() {
    let mut policy = policy();
    assert_eq!(
        policy.plan(&History::default(), target(5), 0).target.size,
        5
    );
    policy.minimum = 2;
    assert_eq!(
        policy.plan(&History::default(), target(2), 0).target.size,
        2
    );
    policy.minimum = 0;
    let history = History {
        demand: vec![(0, 100)],
        startup_ms: vec![8_000],
    };
    assert_eq!(policy.plan(&history, target(20), 0).target.size, 0);
}

#[test]
fn capacity_covers_consumption_during_the_measured_replacement_time() {
    let history = History {
        demand: (0..60).map(|second| (second, 2)).collect(),
        startup_ms: vec![8_000; 20],
    };
    let plan = policy().plan(&history, target(5), 0);
    assert_eq!(plan.horizon_seconds, 9);
    assert_eq!(plan.target.size, 28);
    let slower = History {
        startup_ms: vec![12_000; 20],
        ..history
    };
    assert_eq!(policy().plan(&slower, target(5), 0).target.size, 32);
}

#[test]
fn recent_bursts_grow_immediately_and_respect_the_cap() {
    let history = History {
        demand: vec![(30, 20)],
        startup_ms: vec![1_000],
    };
    assert_eq!(policy().plan(&history, target(5), 0).target.size, 21);
    let history = History {
        demand: vec![(0, 500)],
        ..history
    };
    assert_eq!(policy().plan(&history, target(5), 0).target.size, 32);
}

#[test]
fn shrinking_waits_for_sustained_lower_demand_then_steps_down() {
    let quiet = History::default();
    let first = policy().plan(&quiet, target(20), 10_000).target;
    assert_eq!(first.size, 20);
    assert_eq!(first.shrink_at_ms, Some(310_000));
    assert_eq!(policy().plan(&quiet, first, 309_999).target.size, 20);
    let next = policy().plan(&quiet, first, 310_000).target;
    assert_eq!(next.size, 18);
    assert_eq!(next.shrink_at_ms, Some(370_000));
    let burst = History {
        demand: vec![(0, 100)],
        startup_ms: vec![],
    };
    let grown = policy().plan(&burst, next, 320_000).target;
    assert_eq!(grown.size, 32);
    assert_eq!(grown.shrink_at_ms, None);
}

#[test]
fn p95_replacement_time_ignores_a_single_outlier_and_bursts_age_out() {
    let mut startup_ms = vec![2_000; 20];
    startup_ms.push(100_000);
    let history = History {
        demand: vec![(300, 100)],
        startup_ms,
    };
    let plan = policy().plan(&history, target(5), 0);
    assert_eq!(plan.horizon_seconds, 3);
    assert_eq!(plan.target.size, 5);
}
