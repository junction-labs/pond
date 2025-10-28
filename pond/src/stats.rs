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
macro_rules! scoped_latency_recorder {
    ($name:literal) => {
        let _guard = RecordLatencyGuard::new($name);
    };
    ($name:literal, $($key:literal => $value:expr),+ $(,)?) => {
        let _guard = RecordLatencyGuard::with_labels($name, &[$(($key, $value)),+]);
    };
}

pub(crate) struct RecordLatencyGuard {
    start: Instant,
    metric: &'static str,
    labels: Option<&'static [(&'static str, &'static str)]>,
}

impl RecordLatencyGuard {
    pub(crate) fn new(metric: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels: None,
        }
    }

    pub(crate) fn with_labels(
        metric: &'static str,
        labels: &'static [(&'static str, &'static str)],
    ) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels: Some(labels),
        }
    }
}

impl Drop for RecordLatencyGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if let Some(labels) = self.labels {
            metrics::histogram!(self.metric, labels).record(elapsed);
        } else {
            metrics::histogram!(self.metric).record(elapsed);
        }
    }
}
