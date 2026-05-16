use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, sleep_until, timeout};
use tokio::time::Instant;

// ── .env loader ────────────────────────────────────────────────────────────

fn load_dotenv() {
    let _ = dotenvy::from_filename("../.env");
    let _ = dotenvy::dotenv();
}

// ── constants ──────────────────────────────────────────────────────────────

const TEST_DURATION_SECS: u64 = 30;
const GRACE_PERIOD_SECS: u64 = 10;
const PUB_QOS_VALUES: [u8; 3] = [0, 1, 2];
const DELAY_VALUES: [u64; 2] = [0, 100];
const MSG_SIZE_VALUES: [usize; 3] = [1, 1000, 4000];
const SUB_QOS_VALUES: [u8; 3] = [0, 1, 2];

/// Expected messages for delay=100ms over TEST_DURATION_SECS seconds.
/// delay=0 is unbounded so we leave it as None.
fn expected_messages(delay_ms: u64) -> Option<usize> {
    if delay_ms == 0 {
        None
    } else {
        // Floor: publisher fires every delay_ms milliseconds
        Some((TEST_DURATION_SECS * 1000 / delay_ms) as usize)
    }
}

/// Initial Vec capacity: generous for delay=0, small for throttled tests.
fn initial_capacity(delay_ms: u64) -> usize {
    if delay_ms == 0 {
        12_000_000
    } else {
        // delay=100ms → ~300 messages over 30s, round up with headroom
        ((TEST_DURATION_SECS * 1000 / delay_ms) as usize + 64).next_power_of_two()
    }
}

// ── CLI filters ────────────────────────────────────────────────────────────

/// Filters parsed from command-line flags. Each field is either "all values"
/// (None) or a whitelist of values the user specified.
///
/// Usage examples:
///   --pub-qos 0,1          only QoS 0 and 1 on the publisher side
///   --sub-qos 2            only QoS 2 on the subscriber side
///   --delay 0              only delay=0ms (full-speed) tests
///   --size 1,4000          only 1B and 4000B message sizes
///
/// Flags can be combined freely; unspecified dimensions run all values.
#[derive(Debug, Default)]
struct Filters {
    pub_qos: Option<Vec<u8>>,
    sub_qos: Option<Vec<u8>>,
    delay_ms: Option<Vec<u64>>,
    msg_size: Option<Vec<usize>>,
}

impl Filters {
    fn from_args() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut f = Filters::default();
        let mut i = 1usize;
        while i < args.len() {
            match args[i].as_str() {
                "--pub-qos" => {
                    f.pub_qos = Some(Self::parse_list(&args, &mut i, "--pub-qos", |s| {
                        s.parse::<u8>().ok().filter(|&v| v <= 2)
                    }));
                }
                "--sub-qos" => {
                    f.sub_qos = Some(Self::parse_list(&args, &mut i, "--sub-qos", |s| {
                        s.parse::<u8>().ok().filter(|&v| v <= 2)
                    }));
                }
                "--delay" => {
                    f.delay_ms = Some(Self::parse_list(&args, &mut i, "--delay", |s| {
                        s.parse::<u64>().ok()
                    }));
                }
                "--size" => {
                    f.msg_size = Some(Self::parse_list(&args, &mut i, "--size", |s| {
                        s.parse::<usize>().ok()
                    }));
                }
                "--help" | "-h" => {
                    println!(
                        "Usage: mqtt-analyser [OPTIONS]\n\
                         \n\
                         Options:\n\
                           --pub-qos <0,1,2>   Publisher QoS values to test (default: all)\n\
                           --sub-qos <0,1,2>   Subscriber QoS values to test (default: all)\n\
                           --delay   <ms,...>  Publish delay values in ms to test (default: all)\n\
                           --size    <b,...>   Message sizes in bytes to test (default: all)\n\
                           -h, --help          Show this help\n\
                         \n\
                         Examples:\n\
                           # Single test: pubQoS=0, subQoS=0, delay=100ms, size=1B\n\
                           mqtt-analyser --pub-qos 0 --sub-qos 0 --delay 100 --size 1\n\
                         \n\
                           # All QoS combos but only delay=0 and 1B messages\n\
                           mqtt-analyser --delay 0 --size 1\n"
                    );
                    std::process::exit(0);
                }
                other => {
                    eprintln!("Unknown argument: {other}  (try --help)");
                    std::process::exit(1);
                }
            }
            i += 1;
        }
        f
    }

    /// Parse a comma-separated value list from the next argument token.
    fn parse_list<T, F>(args: &[String], i: &mut usize, flag: &str, parse: F) -> Vec<T>
    where
        F: Fn(&str) -> Option<T>,
    {
        *i += 1;
        let raw = args.get(*i).unwrap_or_else(|| {
            eprintln!("Flag {flag} requires a value");
            std::process::exit(1);
        });
        let values: Vec<T> = raw.split(',').filter_map(|s| parse(s.trim())).collect();
        if values.is_empty() {
            eprintln!("Flag {flag}: no valid values in '{raw}'");
            std::process::exit(1);
        }
        values
    }

    fn matches(&self, pub_qos: u8, sub_qos: u8, delay_ms: u64, msg_size: usize) -> bool {
        self.pub_qos.as_ref().map_or(true, |v| v.contains(&pub_qos))
            && self.sub_qos.as_ref().map_or(true, |v| v.contains(&sub_qos))
            && self.delay_ms.as_ref().map_or(true, |v| v.contains(&delay_ms))
            && self.msg_size.as_ref().map_or(true, |v| v.contains(&msg_size))
    }
}

fn broker_host() -> String {
    env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn broker_port() -> u16 {
    env::var("MQTT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1883)
}

fn to_qos(n: u8) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn now_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_micros()
}

/// Parse a counter message payload directly from bytes: `{seq}:{timestamp_us}:{padding}`
/// Zero-allocation — uses `from_utf8` on slices, no String creation.
fn parse_counter_payload(payload: &[u8]) -> Option<Message> {
    let now = now_us();

    let colon1 = payload.iter().position(|&b| b == b':')?;
    let seq = std::str::from_utf8(&payload[..colon1]).ok()?.parse::<u64>().ok()?;

    let rest = &payload[colon1 + 1..];
    let colon2 = rest.iter().position(|&b| b == b':')?;
    let sent_us = std::str::from_utf8(&rest[..colon2]).ok()?.parse::<u128>().ok()?;

    Some(Message {
        seq,
        sent_us,
        recv_us: now,
        latency_us: (now as i64) - (sent_us as i64),
    })
}

// ── data structures ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Message {
    seq: u64,
    sent_us: u128,
    recv_us: u128,
    latency_us: i64,
}

#[derive(Debug, Clone, Serialize)]
struct TestResult {
    pub_qos: u8,
    sub_qos: u8,
    delay_ms: u64,
    msg_size: usize,
    messages_received: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_expected: Option<usize>,
    elapsed_s: f64,
    throughput_msg_s: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_us_min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_us_max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_us_avg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_us_p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_us_p99: Option<f64>,
    done_received: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// ── helpers ────────────────────────────────────────────────────────────────

/// Compute p50 and p99 in a single sort — avoids cloning the vec twice.
fn percentiles_p50_p99(mut data: Vec<i64>) -> (Option<f64>, Option<f64>) {
    if data.is_empty() {
        return (None, None);
    }
    data.sort_unstable();
    let n = data.len();

    let interp = |p: f64| -> f64 {
        let rank = (p / 100.0) * (n as f64 - 1.0);
        let lo = rank as usize;
        let hi = (lo + 1).min(n - 1);
        let frac = rank - lo as f64;
        data[lo] as f64 + frac * (data[hi] as f64 - data[lo] as f64)
    };

    let p50 = (interp(50.0) * 10.0).round() / 10.0;
    let p99 = (interp(99.0) * 10.0).round() / 10.0;
    (Some(p50), Some(p99))
}

fn mean(data: &[i64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    let sum: i64 = data.iter().sum();
    Some(sum as f64 / data.len() as f64)
}

// ── analyser ───────────────────────────────────────────────────────────────

struct Analyser {
    host: String,
    port: u16,
}

impl Analyser {
    fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Run a single test: connect → subscribe → configure publisher → 30s burst → stats.
    async fn run_one_test(
        &self,
        pub_qos: u8,
        delay_ms: u64,
        msg_size: usize,
        sub_qos: u8,
    ) -> TestResult {
        let client_id = format!("analyser-{pub_qos}-{delay_ms}-{msg_size}-{sub_qos}");
        let mut mqtt_opts = MqttOptions::new(&client_id, &self.host, self.port);
        mqtt_opts.set_keep_alive(Duration::from_secs(60));
        // FIX 4: Always start with a clean session so no queued QoS 1/2 messages
        // from a previous run are replayed into this test's stats.
        mqtt_opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 65_535);

        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Vec<Message>>();
        let done_flag = Arc::new(AtomicBool::new(false));
        let done_flag_ev = done_flag.clone();

        // Rendezvous: event loop signals us once both SubAcks are confirmed.
        // This replaces all blind sleeps for the connection/subscribe phase.
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        // ── spawn event loop ─────────────────────────────────────────────
        // We need to count SubAcks: we subscribed to 2 topics, so wait for 2.
        let ev_handle = tokio::spawn(async move {
            const BATCH_SIZE: usize = 4096;
            let mut batch: Vec<Message> = Vec::with_capacity(BATCH_SIZE);
            let mut ready_tx = Some(ready_tx);
            let mut subacks_remaining: u8 = 2; // counter_topic + request/go

            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::SubAck(_))) => {
                        if subacks_remaining > 0 {
                            subacks_remaining -= 1;
                            if subacks_remaining == 0 {
                                // Both subscriptions confirmed — unblock the main task
                                if let Some(tx) = ready_tx.take() {
                                    let _ = tx.send(());
                                }
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        // Only treat payload "done" on request/go as the test-end signal.
                        // Ignore "start"/"stop" echoes on the same topic.
                        if p.topic.as_bytes() == b"request/go" {
                            if p.payload.as_ref() == b"done" {
                                done_flag_ev.store(true, Ordering::Release);
                            }
                            continue;
                        }

                        if p.topic.as_bytes().starts_with(b"counter/") {
                            if let Some(msg) = parse_counter_payload(&p.payload) {
                                batch.push(msg);
                                if batch.len() >= BATCH_SIZE {
                                    let full = std::mem::replace(
                                        &mut batch,
                                        Vec::with_capacity(BATCH_SIZE),
                                    );
                                    if batch_tx.send(full).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[eventloop] error: {e}");
                        break;
                    }
                }
            }

            // Flush remaining batch before exit
            if !batch.is_empty() {
                let _ = batch_tx.send(batch);
            }
        });

        // ── subscribe ────────────────────────────────────────────────────
        // Subscribe *after* spawning the event loop so it can process ConnAck
        // and SubAck. The event loop signals ready_rx once both SubAcks arrive.
        let counter_topic = format!("counter/{pub_qos}/{delay_ms}/{msg_size}");
        let _ = client.subscribe(&counter_topic, to_qos(sub_qos)).await;
        let _ = client.subscribe("request/go", to_qos(sub_qos)).await;

        // Block until both SubAcks are confirmed, with a 5s safety timeout.
        if timeout(Duration::from_secs(5), ready_rx).await.is_err() {
            eprintln!("  ⚠  WARNING: timed out waiting for SubAck — broker may be overloaded");
        }

        // Stop any publisher left running from the previous test *after* we are
        // confirmed subscribed, so we don't miss the stop echo on request/go.
        let _ = client
            .publish("request/go", QoS::AtLeastOnce, false, "stop")
            .await;
        // Brief pause for the publisher to receive and honour "stop".
        sleep(Duration::from_millis(200)).await;

        // ── configure publisher (atomic, single topic) ───────────────────
        // Sending all config in one message on one topic avoids cross-topic
        // ordering races on high-latency links where "start" could arrive
        // before "delay=100ms", causing the publisher to run with defaults.
        let config_payload = format!("{pub_qos}:{delay_ms}:{msg_size}");
        let _ = client
            .publish("request/config", QoS::AtLeastOnce, false, &config_payload)
            .await;

        // Brief sleep for the config to reach the remote publisher before "start"
        sleep(Duration::from_millis(200)).await;

        // ── start test ───────────────────────────────────────────────────
        let start = Instant::now();
        let _ = client
            .publish("request/go", QoS::AtLeastOnce, false, "start")
            .await;

        let collect_end = start + Duration::from_secs(TEST_DURATION_SECS + GRACE_PERIOD_SECS);

        // FIX 6: Allocate based on expected volume rather than always 12M.
        let mut messages: Vec<Message> = Vec::with_capacity(initial_capacity(delay_ms));

        loop {
            tokio::select! {
                maybe_batch = batch_rx.recv() => {
                    match maybe_batch {
                        Some(batch) => messages.extend(batch),
                        None => break,
                    }
                }
                _ = sleep_until(collect_end) => {
                    break;
                }
            }
        }

        // Drain any remaining batches (window has closed, channel still open)
        while let Ok(batch) = batch_rx.try_recv() {
            messages.extend(batch);
        }

        let elapsed = start.elapsed().as_secs_f64();
        let done_received = done_flag.load(Ordering::Acquire);

        // ── clean up ─────────────────────────────────────────────────────
        let _ = client.unsubscribe(&counter_topic).await;
        let _ = client.unsubscribe("request/go").await;

        // Drop the client then wait for the event loop to flush its final
        // in-flight batch before aborting. Especially important for QoS 2,
        // which needs one extra poll cycle for PUBCOMP.
        drop(client);
        sleep(Duration::from_millis(400)).await;

        // Drain the last batch the event loop sent after client disconnect
        while let Ok(batch) = batch_rx.try_recv() {
            messages.extend(batch);
        }

        // Safe to abort now — event loop has already flushed
        ev_handle.abort();

        // ── stats ────────────────────────────────────────────────────────
        let count = messages.len();
        let latencies: Vec<i64> = messages.iter().map(|m| m.latency_us).collect();

        if count == 0 {
            eprintln!("  ⚠  WARNING: 0 messages received!");
        }

        // Compute p50 and p99 in a single sort pass (no clone needed for both)
        let (p50, p99) = percentiles_p50_p99(latencies.clone());

        TestResult {
            pub_qos,
            sub_qos,
            delay_ms,
            msg_size,
            messages_received: count,
            messages_expected: expected_messages(delay_ms),
            elapsed_s: (elapsed * 100.0).round() / 100.0,
            throughput_msg_s: if elapsed > 0.0 && count > 0 {
                ((count as f64 / elapsed) * 10.0).round() / 10.0
            } else {
                0.0
            },
            latency_us_min: latencies.iter().min().copied(),
            latency_us_max: latencies.iter().max().copied(),
            latency_us_avg: mean(&latencies).map(|v| (v * 10.0).round() / 10.0),
            latency_us_p50: p50,
            latency_us_p99: p99,
            done_received,
            error: None,
        }
    }

    /// Run test combinations, optionally filtered by CLI flags.
    async fn run_all(&self, filters: &Filters) -> Vec<TestResult> {
        // Build the filtered matrix up-front so we know the total count.
        let matrix: Vec<(u8, u64, usize, u8)> = PUB_QOS_VALUES
            .iter()
            .flat_map(|&pq| {
                DELAY_VALUES.iter().flat_map(move |&d| {
                    MSG_SIZE_VALUES.iter().flat_map(move |&sz| {
                        SUB_QOS_VALUES.iter().map(move |&sq| (pq, d, sz, sq))
                    })
                })
            })
            .filter(|&(pq, d, sz, sq)| filters.matches(pq, sq, d, sz))
            .collect();

        let total = matrix.len();
        if total == 0 {
            eprintln!("No tests match the specified filters.");
            return Vec::new();
        }
        println!("Starting {total} test(s) ({TEST_DURATION_SECS}s each) …\n");

        let mut results = Vec::with_capacity(total);

        for (test_num, (pub_qos, delay_ms, msg_size, sub_qos)) in matrix.into_iter().enumerate() {
            let label = format!(
                "pubQoS={pub_qos}  subQoS={sub_qos}  delay={delay_ms}ms  size={msg_size}B"
            );
            print!("[{:>width$}/{total}] {label} … ", test_num + 1, width = total.to_string().len());
            let _ = std::io::stdout().flush();

            let result = self
                .run_one_test(pub_qos, delay_ms, msg_size, sub_qos)
                .await;

            let status = format!(
                "{} msgs  {}/s  ",
                result.messages_received, result.throughput_msg_s
            );
            if let (Some(avg), Some(p99)) = (result.latency_us_avg, result.latency_us_p99) {
                println!("{status}avg={avg}µs  p99={p99}µs");
            } else {
                println!("{status}");
            }

            results.push(result);
            if test_num + 1 < total {
                sleep(Duration::from_secs(1)).await;
            }
        }

        results
    }
}

// ── summary printer ────────────────────────────────────────────────────────

fn print_summary(results: &[TestResult]) {
    println!("\n{}", "=".repeat(100));
    println!(
        "{:>5} {:>5} {:>6} {:>5}  {:>7} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "pubQ", "subQ", "delay", "size", "msgs", "msg/s", "avg(µs)", "p99(µs)", "min(µs)", "max(µs)"
    );
    println!("{}", "-".repeat(100));

    for r in results {
        if let Some(ref err) = r.error {
            println!(
                "{:>5} {:>5} {:>5}ms {:>4}B    ERROR: {err}",
                r.pub_qos, r.sub_qos, r.delay_ms, r.msg_size
            );
        } else {
            let avg = r
                .latency_us_avg
                .map_or("N/A".to_string(), |v| v.to_string());
            let p99 = r
                .latency_us_p99
                .map_or("N/A".to_string(), |v| v.to_string());
            let min = r
                .latency_us_min
                .map_or("N/A".to_string(), |v| v.to_string());
            let max = r
                .latency_us_max
                .map_or("N/A".to_string(), |v| v.to_string());
            println!(
                "{:>5} {:>5} {:>5}ms {:>4}B  {:>7} {:>7.0}/s {:>10} {:>10} {:>10} {:>10}",
                r.pub_qos,
                r.sub_qos,
                r.delay_ms,
                r.msg_size,
                r.messages_received,
                r.throughput_msg_s,
                avg,
                p99,
                min,
                max
            );
        }
    }
    println!("{}", "=".repeat(100));
}

// ── main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    load_dotenv();

    let filters = Filters::from_args();

    let host = broker_host();
    let port = broker_port();
    println!("Connecting to MQTT broker at {host}:{port}");

    let analyser = Analyser::new(host, port);
    let results = analyser.run_all(&filters).await;

    print_summary(&results);

    match serde_json::to_string_pretty(&results) {
        Ok(json) => {
            if let Err(e) = std::fs::write("results.json", &json) {
                eprintln!("Failed to write results.json: {e}");
            } else {
                println!("\nSaved results to results.json");
            }
        }
        Err(e) => {
            eprintln!("Failed to serialize results: {e}");
        }
    }
}