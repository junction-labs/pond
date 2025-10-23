use std::{os::unix::fs::OpenOptionsExt, path::Path};

use tracing_subscriber::{
    EnvFilter, Registry, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

pub(crate) fn init_logging(debug: bool, stdout: bool, file: Option<&Path>) -> std::io::Result<()> {
    let (reload, filter) = if debug {
        ReloadHandle::from_filters(vec![debug_filter(), normal_filter()])
    } else {
        ReloadHandle::from_filters(vec![normal_filter(), debug_filter()])
    };
    reload.run_signal_handler()?;

    let console_layer = if stdout {
        Some(tracing_subscriber::fmt::layer().compact())
    } else {
        None
    };

    let file_layer = match file {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .mode(0o640)
                .append(true)
                .create(true)
                .open(path)?;
            let layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(false)
                .with_writer(file);
            Some(layer)
        }
        None => None,
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(console_layer)
        .init();

    Ok(())
}

fn debug_filter() -> EnvFilter {
    EnvFilter::new("fuser::request=trace,object_store=debug,pond=info")
}

fn normal_filter() -> EnvFilter {
    EnvFilter::new("pond=info")
}

struct ReloadHandle {
    handle: reload::Handle<EnvFilter, Registry>,
    idx: usize,
    filters: Vec<EnvFilter>,
}

impl ReloadHandle {
    fn from_filters(filters: Vec<EnvFilter>) -> (Self, reload::Layer<EnvFilter, Registry>) {
        let (filter, handle) = reload::Layer::new(filters[0].clone());
        (
            Self {
                handle,
                filters,
                idx: 0,
            },
            filter,
        )
    }

    fn run_signal_handler(mut self) -> std::io::Result<()> {
        let mut signals = signal_hook::iterator::Signals::new([signal_hook::consts::SIGUSR2])?;
        std::thread::spawn(move || {
            for _ in signals.forever() {
                self.load_next();
            }
        });

        Ok(())
    }

    fn load_next(&mut self) {
        let next_idx = (self.idx + 1) % self.filters.len();
        let _ = self.handle.modify(|f| *f = self.filters[next_idx].clone());
        self.idx = next_idx;
    }
}
