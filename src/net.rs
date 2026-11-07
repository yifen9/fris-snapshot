use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::Url;

pub const FLOOR: Duration = Duration::from_secs(1);
const CHUNK: usize = 64 * 1024;
const POLL: Duration = Duration::from_secs(5);
const PAUSE_NAP: Duration = Duration::from_secs(5);
const USER_AGENT: &str = concat!("fris-snapshot/", env!("CARGO_PKG_VERSION"));

pub fn parse_rate(text: &str) -> Option<u64> {
    let t = text
        .trim()
        .trim_end_matches("/s")
        .trim_end_matches(['B', 'b']);
    let t = t.trim_end_matches('i');
    let (num, mult) = match t.chars().last()? {
        'k' | 'K' => (&t[..t.len() - 1], 1024.0),
        'm' | 'M' => (&t[..t.len() - 1], 1024.0 * 1024.0),
        'g' | 'G' => (&t[..t.len() - 1], 1024.0 * 1024.0 * 1024.0),
        _ => (t, 1.0),
    };
    let value: f64 = num.trim().parse().ok()?;
    (value >= 0.0).then_some((value * mult) as u64)
}

pub fn backoff(base: u64, cap: u64, attempt: u32) -> u64 {
    base.saturating_mul(1u64 << attempt.saturating_sub(1).min(20))
        .min(cap)
}

pub fn format_rate(rate: u64) -> String {
    if rate == 0 {
        return "unlimited".into();
    }
    format!("{:.2} MiB/s", rate as f64 / 1024.0 / 1024.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    Go,
    Stop,
}

struct Slot {
    interval: Duration,
    next: Instant,
}

fn host(url: &str) -> (String, bool) {
    let Ok(parsed) = Url::parse(url) else {
        return (url.to_owned(), false);
    };
    let name = parsed.host_str().unwrap_or(url).to_owned();
    let local = name.eq_ignore_ascii_case("localhost")
        || name
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    (name, local)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Bandwidth,
    Pause,
    Stop,
}

impl Signal {
    pub fn path(self, control: &Path) -> PathBuf {
        control.join(match self {
            Self::Bandwidth => "bandwidth",
            Self::Pause => "pause",
            Self::Stop => "stop",
        })
    }
}

pub struct Gate {
    control: PathBuf,
    default_rate: u64,
    rate: AtomicU64,
    min_interval: Duration,
    hosts: Mutex<HashMap<String, Slot>>,
    next_free: Mutex<Instant>,
    last_poll: Mutex<Option<Instant>>,
    received: AtomicU64,
}

impl Gate {
    pub fn new(control: &Path, default_rate: u64, min_interval: Duration) -> Result<Self> {
        fs::create_dir_all(control).with_context(|| format!("create {}", control.display()))?;
        let gate = Self {
            control: control.to_path_buf(),
            default_rate,
            rate: AtomicU64::new(default_rate),
            min_interval,
            hosts: Mutex::new(HashMap::new()),
            next_free: Mutex::new(Instant::now()),
            last_poll: Mutex::new(None),
            received: AtomicU64::new(0),
        };
        gate.refresh();
        Ok(gate)
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    fn signal(&self, signal: Signal) -> PathBuf {
        signal.path(&self.control)
    }

    pub fn clear_stop(&self) {
        let _ = fs::remove_file(self.signal(Signal::Stop));
    }

    fn refresh(&self) {
        let rate = fs::read_to_string(self.signal(Signal::Bandwidth))
            .ok()
            .and_then(|t| parse_rate(&t))
            .unwrap_or(self.default_rate);
        self.rate.store(rate, Ordering::Relaxed);
    }

    fn poll(&self) {
        let mut last = self
            .last_poll
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last.is_none_or(|t| t.elapsed() >= POLL) {
            self.refresh();
            *last = Some(Instant::now());
        }
    }

    pub fn stopping(&self) -> bool {
        self.signal(Signal::Stop).exists()
    }

    pub fn pause(&self, secs: u64) -> Admit {
        for _ in 0..secs {
            if self.stopping() {
                return Admit::Stop;
            }
            sleep(Duration::from_secs(1));
        }
        Admit::Go
    }

    fn slot<T>(&self, url: &str, f: impl FnOnce(&mut Slot) -> T) -> T {
        let (name, local) = host(url);
        let floor = if local { Duration::ZERO } else { FLOOR };
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = hosts.entry(name).or_insert_with(|| Slot {
            interval: self.min_interval.max(floor),
            next: Instant::now(),
        });
        f(slot)
    }

    pub fn admit(&self, url: &str) -> Admit {
        loop {
            self.poll();
            if self.stopping() {
                return Admit::Stop;
            }
            if !self.signal(Signal::Pause).exists() {
                break;
            }
            sleep(PAUSE_NAP);
        }
        let start = self.slot(url, |s| {
            let start = s.next.max(Instant::now());
            s.next = start + s.interval;
            start
        });
        loop {
            let left = start.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Admit::Go;
            }
            if self.stopping() {
                return Admit::Stop;
            }
            sleep(left.min(Duration::from_secs(1)));
        }
    }

    fn consume(&self, n: usize) {
        self.received.fetch_add(n as u64, Ordering::Relaxed);
        self.poll();
        let rate = self.rate();
        if rate == 0 {
            return;
        }
        let cost = Duration::from_secs_f64(n as f64 / rate as f64);
        let wait = {
            let mut next = self
                .next_free
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            let start = (*next).max(now);
            *next = start + cost;
            start.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            sleep(wait);
        }
    }

    pub fn read_body(&self, mut response: Response) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(response.content_length().unwrap_or(0) as usize);
        let mut buf = vec![0u8; CHUNK];
        loop {
            let n = response.read(&mut buf).context("read body")?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            self.consume(n);
        }
        Ok(out)
    }
}

pub fn client(cookies: bool, timeout: Duration) -> Result<Client> {
    Client::builder()
        .user_agent(USER_AGENT)
        .cookie_store(cookies)
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_mins(1))
        .build()
        .context("build http client")
}

pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
    pub ttfb: Duration,
    pub elapsed: Duration,
}

fn send(
    gate: &Gate,
    mut request: RequestBuilder,
    headers: &[(&str, &str)],
    method: &str,
    url: &str,
) -> Result<Reply> {
    let started = Instant::now();
    for (k, v) in headers {
        request = request.header(*k, *v);
    }
    let response = request.send().with_context(|| format!("{method} {url}"))?;
    let ttfb = started.elapsed();
    let status = response.status().as_u16();
    let body = gate.read_body(response)?;
    Ok(Reply {
        status,
        body,
        ttfb,
        elapsed: started.elapsed(),
    })
}

pub fn post(
    gate: &Gate,
    client: &Client,
    url: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Result<Reply> {
    send(gate, client.post(url).body(body), headers, "POST", url)
}

pub fn get(gate: &Gate, client: &Client, url: &str, headers: &[(&str, &str)]) -> Result<Reply> {
    send(gate, client.get(url), headers, "GET", url)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{backoff, parse_rate, Admit, Gate, Signal, FLOOR};

    fn gate(tag: &str, min_interval: Duration) -> anyhow::Result<Gate> {
        let dir = std::env::temp_dir().join(format!("fris-net-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let gate = Gate::new(&dir, 0, min_interval)?;
        std::fs::remove_dir_all(&dir)?;
        Ok(gate)
    }

    #[test]
    fn workers_on_one_host_share_its_spacing() -> anyhow::Result<()> {
        let g = Arc::new(gate("shared", Duration::ZERO)?);
        let began = Instant::now();
        let workers: Vec<_> = (0..3)
            .map(|i| {
                let g = Arc::clone(&g);
                std::thread::spawn(move || {
                    let host = if i < 2 {
                        "a.example.org"
                    } else {
                        "b.example.org"
                    };
                    assert_eq!(g.admit(&format!("https://{host}/p{i}")), Admit::Go);
                    (host, began.elapsed())
                })
            })
            .collect();
        let mut times: Vec<_> = workers
            .into_iter()
            .map(|w| w.join().expect("worker"))
            .collect();
        times.sort();
        let a: Vec<Duration> = times
            .iter()
            .filter(|t| t.0 == "a.example.org")
            .map(|t| t.1)
            .collect();
        assert!(a[1].saturating_sub(a[0]) + Duration::from_millis(20) >= FLOOR);
        let b = times.iter().find(|t| t.0 == "b.example.org").expect("b").1;
        assert!(b < FLOOR / 2);
        Ok(())
    }

    #[test]
    fn backoff_doubles_from_the_base_up_to_the_cap() {
        let secs: Vec<u64> = (0..6).map(|a| backoff(5, 30, a)).collect();
        assert_eq!(secs, [5, 5, 10, 20, 30, 30]);
        assert_eq!(backoff(u64::MAX, u64::MAX, u32::MAX), u64::MAX);
    }

    #[test]
    fn pauses_end_early_on_stop() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-net-pause-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let g = Gate::new(&dir, 0, Duration::ZERO)?;
        assert_eq!(g.pause(0), Admit::Go);
        std::fs::write(Signal::Stop.path(&dir), "")?;
        let began = Instant::now();
        assert_eq!(g.pause(60), Admit::Stop);
        assert!(began.elapsed() < Duration::from_secs(1));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn rates() {
        assert_eq!(parse_rate("2M"), Some(2 * 1024 * 1024));
        assert_eq!(parse_rate("2MiB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_rate("1.5MB/s"), Some(1_572_864));
        assert_eq!(parse_rate("512K"), Some(512 * 1024));
        assert_eq!(parse_rate("0"), Some(0));
        assert_eq!(parse_rate("1000"), Some(1000));
        assert_eq!(parse_rate("x"), None);
    }
}
