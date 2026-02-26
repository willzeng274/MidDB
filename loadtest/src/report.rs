use hdrhistogram::Histogram;
use std::time::{Duration, Instant};

pub struct LoadTestResult {
    pub name: String,
    pub total_ops: u64,
    pub elapsed: Duration,
    pub histogram: Histogram<u64>,
    pub errors: u64,
}

impl LoadTestResult {
    pub fn print(&self) {
        let ops_per_sec = self.total_ops as f64 / self.elapsed.as_secs_f64();
        let p50 = self.histogram.value_at_quantile(0.50);
        let p95 = self.histogram.value_at_quantile(0.95);
        let p99 = self.histogram.value_at_quantile(0.99);
        let p999 = self.histogram.value_at_quantile(0.999);
        let max = self.histogram.max();
        let mean = self.histogram.mean();

        println!("  {}", self.name);
        println!("    ops: {:>10}  |  elapsed: {:.2}s  |  throughput: {:.0} ops/s",
            self.total_ops, self.elapsed.as_secs_f64(), ops_per_sec);
        println!("    latency (μs): mean={:.0}  p50={}  p95={}  p99={}  p99.9={}  max={}",
            mean, p50, p95, p99, p999, max);
        if self.errors > 0 {
            println!("    errors: {}", self.errors);
        }
        println!();
    }
}

pub struct LoadTestRunner {
    pub name: String,
    hist: Histogram<u64>,
    total_ops: u64,
    errors: u64,
    start: Option<Instant>,
    elapsed: Duration,
}

impl LoadTestRunner {
    pub fn new(name: &str) -> Self {
        LoadTestRunner {
            name: name.to_string(),
            hist: Histogram::new(3).unwrap(),
            total_ops: 0,
            errors: 0,
            start: None,
            elapsed: Duration::ZERO,
        }
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn record_op(&mut self, duration: Duration) {
        let us = duration.as_micros() as u64;
        let _ = self.hist.record(us.max(1));
        self.total_ops += 1;
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    pub fn finish(mut self) -> LoadTestResult {
        self.elapsed = self.start.map(|s| s.elapsed()).unwrap_or(Duration::ZERO);
        LoadTestResult {
            name: self.name,
            total_ops: self.total_ops,
            elapsed: self.elapsed,
            histogram: self.hist,
            errors: self.errors,
        }
    }
}
