//! Counters and latencies in Prometheus' text format, at `GET /metrics`: agent-pi's design names
//! time to first token, run duration and tool latency per server as what to measure. In memory,
//! starting again at each restart, as Prometheus expects of counters.
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;
use std::time::Duration;

/// Upper bounds in seconds: voice cares about the first second, and a run can take minutes.
const BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
];

#[derive(Clone, Default)]
struct Histogram {
    counts: [u64; BUCKETS.len()],
    count: u64,
    sum: f64,
}

/// A metric's labels, as written: `agent="ti",status="completed"`.
type Labels = String;

#[derive(Default)]
struct Inner {
    counters: BTreeMap<(&'static str, Labels), u64>,
    histograms: BTreeMap<(&'static str, Labels), Histogram>,
}

#[derive(Default)]
pub struct Metrics {
    inner: Mutex<Inner>,
}

/// A label value, escaped for the text format.
fn value(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let written: Vec<String> = pairs
        .iter()
        .map(|(name, text)| format!("{name}=\"{}\"", value(text)))
        .collect();
    written.join(",")
}

impl Metrics {
    pub fn add(&self, name: &'static str, pairs: &[(&str, &str)], by: u64) {
        let key = (name, labels(pairs));
        *self.inner.lock().unwrap().counters.entry(key).or_default() += by;
    }
    pub fn observe(&self, name: &'static str, pairs: &[(&str, &str)], took: Duration) {
        let seconds = took.as_secs_f64();
        let key = (name, labels(pairs));
        let mut inner = self.inner.lock().unwrap();
        let histogram = inner.histograms.entry(key).or_default();
        histogram.count += 1;
        histogram.sum += seconds;
        for (slot, bound) in histogram.counts.iter_mut().zip(BUCKETS) {
            if seconds <= bound {
                *slot += 1;
            }
        }
    }
    /// Everything so far, with `gauges` read at the moment of asking: name, labels, value.
    pub fn render(&self, gauges: &[(&str, Labels, f64)]) -> String {
        let inner = self.inner.lock().unwrap();
        let mut out = String::new();
        let typed = |out: &mut String, name: &str, kind: &str| {
            let _ = writeln!(out, "# TYPE {name} {kind}");
        };
        let mut last = "";
        for ((name, tags), count) in &inner.counters {
            if *name != last {
                typed(&mut out, name, "counter");
                last = name;
            }
            let _ = writeln!(out, "{name}{{{tags}}} {count}");
        }
        last = "";
        for ((name, tags), histogram) in &inner.histograms {
            if *name != last {
                typed(&mut out, name, "histogram");
                last = name;
            }
            let joined = if tags.is_empty() {
                String::new()
            } else {
                format!("{tags},")
            };
            for (bound, count) in BUCKETS.iter().zip(histogram.counts) {
                let _ = writeln!(out, "{name}_bucket{{{joined}le=\"{bound}\"}} {count}");
            }
            let _ = writeln!(
                out,
                "{name}_bucket{{{joined}le=\"+Inf\"}} {}",
                histogram.count
            );
            let _ = writeln!(out, "{name}_sum{{{tags}}} {}", histogram.sum);
            let _ = writeln!(out, "{name}_count{{{tags}}} {}", histogram.count);
        }
        let mut last = "";
        for (name, tags, reading) in gauges {
            if *name != last {
                typed(&mut out, name, "gauge");
                last = name;
            }
            let _ = writeln!(out, "{name}{{{tags}}} {reading}");
        }
        out
    }
}

/// A gauge's labels.
pub fn gauge_labels(pairs: &[(&str, &str)]) -> Labels {
    labels(pairs)
}
