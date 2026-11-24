/// CPU time this process has used so far, user and system, in milliseconds; `None` off Linux.
pub(crate) fn cpu_ms() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        use nix::sys::resource::{UsageWho, getrusage};
        let usage = getrusage(UsageWho::RUSAGE_SELF).ok()?;
        let ms = |time: nix::sys::time::TimeVal| {
            time.tv_sec() as f64 * 1000.0 + time.tv_usec() as f64 / 1000.0
        };
        Some(ms(usage.user_time()) + ms(usage.system_time()))
    }
    #[cfg(not(target_os = "linux"))]
    None
}

/// The CPU time this process used since `before`, a reading of [`cpu_ms`].
pub(crate) fn cpu_since(before: Option<f64>) -> Option<f64> {
    Some(cpu_ms()? - before?)
}
