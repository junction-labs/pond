use std::time::Instant;

#[macro_export]
macro_rules! scoped_timer {
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let hist = metrics::histogram!($name $(, $label_key $(=> $label_value)?)*);
        RecordLatencyGuard::new(hist)
    }};
}

pub(crate) struct RecordLatencyGuard {
    hist: metrics::Histogram,
    start: Instant,
}

impl RecordLatencyGuard {
    pub(crate) fn new(hist: metrics::Histogram) -> Self {
        let start = Instant::now();
        Self { hist, start }
    }
}

impl Drop for RecordLatencyGuard {
    fn drop(&mut self) {
        self.hist.record(self.start.elapsed().as_secs_f64());
    }
}
