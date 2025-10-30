use std::{sync::LazyLock, time::Instant};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;

// TODO: it might be worthwhile implementing our own recorder if we don't care about people wanting
//       viz. plain text stats might look better.

/// Handle to the global Prometheus Recorder.
pub(crate) static METRICS_HANDLE: LazyLock<PrometheusHandle> = LazyLock::new(|| {
    let builder = PrometheusBuilder::new();
    builder
        .idle_timeout(MetricKindMask::ALL, None)
        .install_recorder()
        // the only failure mode is failing to set the global prom recorder.
        .expect("should be able to install global prometheus recorder")
});

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
