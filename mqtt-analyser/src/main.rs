use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{sleep, sleep_until};
use tokio::time::Instant;

// ── .env loader ────────────────────────────────────────────────────────────

fn load_dotenv() {
    // Try ../.env first (parent dir, since we run from mqtt-analyser/), then .env
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

    // Find first ':'
    let colon1 = payload.iter().position(|&b| b == b':')?;
    let seq = std::str::from_utf8(&payload[..colon1]).ok()?.parse::<u64>().ok()?;

    // Find second ':'
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

fn percentile(mut data: Vec<i64>, p: f64) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    data.sort_unstable();
    let n = data.len();
    let rank = (p / 100.0) * (n as f64 - 1.0);
    let lo = rank as usize;
    let hi = lo + 1;
    if hi >= n {
        return Some(data[n - 1] as f64);
}
    let frac = rank - lo as f64;
    Some(data[lo] as f64 + frac * (data[hi] as f64 - data[lo] as f64))
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
    /// Uses a multi-threaded approach: the event loop parses messages on its own thread
    /// and sends them in batches to avoid channel overhead and backpressure.
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

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 65_535);

        // Batched channel: event loop sends Vec<Message> batches (reduces channel ops ~4096x)
        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Vec<Message>>();
        let done_flag = Arc::new(AtomicBool::new(false));
        let done_flag_ev = done_flag.clone();

        // ── spawn event loop on a separate thread ────────────────────────
        // IMPORTANT: eventloop.poll() must never be interrupted (no select!/timeout).
        // Cancelling poll() mid-operation (e.g. during TCP connect to a remote
        // broker) breaks the MQTT connection and silently drops queued publishes.
        let ev_handle = tokio::spawn(async move {
            const BATCH_SIZE: usize = 4096;
            let mut batch: Vec<Message> = Vec::with_capacity(BATCH_SIZE);

            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        // Detect "done" signal — zero-allocation byte check
                        if p.topic.as_bytes() == b"request/go"
                            && p.payload.len() >= 4
                            && &p.payload[..4] == b"done"
                        {
                            done_flag_ev.store(true, Ordering::Release);
                            continue;
                        }

                        // Parse counter message directly from &[u8] — zero allocation
                        if p.topic.as_bytes().starts_with(b"counter/") {
                            if let Some(msg) = parse_counter_payload(&p.payload) {
                                batch.push(msg);
                                if batch.len() >= BATCH_SIZE {
                                    let full = std::mem::replace(
                                        &mut batch,
                                        Vec::with_capacity(BATCH_SIZE),
                                    );
                                    if batch_tx.send(full).is_err() {
                                        return; // receiver dropped
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => {} // ConnAck, SubAck, PingResp, etc.
                    Err(e) => {
                        eprintln!("[eventloop] error: {e}");
                        break;
                    }
                }
            }

            // Flush remaining on exit
            if !batch.is_empty() {
                let _ = batch_tx.send(batch);
            }
        });

        // ── subscribe ────────────────────────────────────────────────────
        let counter_topic = format!("counter/{pub_qos}/{delay_ms}/{msg_size}");
        let _ = client.subscribe(&counter_topic, to_qos(sub_qos)).await;
        let _ = client.subscribe("request/go", to_qos(sub_qos)).await;

        sleep(Duration::from_millis(300)).await;

        // ── configure publisher ──────────────────────────────────────────
        let _ = client
            .publish("request/qos", QoS::AtLeastOnce, false, pub_qos.to_string())
            .await;
        let _ = client
            .publish("request/delay", QoS::AtLeastOnce, false, delay_ms.to_string())
            .await;
        let _ = client
            .publish(
                "request/messagesize",
                QoS::AtLeastOnce,
                false,
                msg_size.to_string(),
            )
            .await;

        sleep(Duration::from_millis(300)).await;

        // ── start test ───────────────────────────────────────────────────
        let start = Instant::now();
        let _ = client.publish("request/go", QoS::AtLeastOnce, false, "start").await;

        let collect_end = start + Duration::from_secs(TEST_DURATION_SECS + GRACE_PERIOD_SECS);

        // Pre-allocate for high-throughput tests (e.g. 10M+ messages)
        let mut messages: Vec<Message> = Vec::with_capacity(12_000_000);

        // ── collect batches with select! instead of per-message timeout ──
        loop {
            tokio::select! {
                maybe_batch = batch_rx.recv() => {
                    match maybe_batch {
                        Some(batch) => messages.extend(batch),
                        None => break, // channel closed
                    }
                }
                _ = sleep_until(collect_end) => {
                    break;
                }
            }
        }

        // Drain any remaining batches
        while let Ok(batch) = batch_rx.try_recv() {
            messages.extend(batch);
        }

        let elapsed = start.elapsed().as_secs_f64();
        let done_received = done_flag.load(Ordering::Acquire);

        // ── clean up ─────────────────────────────────────────────────────
        // Drop client first → event loop gets disconnect error → flushes final batch
        let _ = client.unsubscribe(&counter_topic).await;
        let _ = client.unsubscribe("request/go").await;
        drop(client);
        sleep(Duration::from_millis(200)).await; // let event loop flush

        // Drain any remaining batches (including the final flush)
        while let Ok(batch) = batch_rx.try_recv() {
            messages.extend(batch);
        }
        ev_handle.abort();

        // ── stats ────────────────────────────────────────────────────────
        let count = messages.len();
        let latencies: Vec<i64> = messages.iter().map(|m| m.latency_us).collect();

        if count == 0 {
            eprintln!("  ⚠  WARNING: 0 messages received!");
        }

        TestResult {
            pub_qos,
            sub_qos,
            delay_ms,
            msg_size,
            messages_received: count,
            messages_expected: None,
            elapsed_s: (elapsed * 100.0).round() / 100.0,
            throughput_msg_s: if elapsed > 0.0 && count > 0 {
                ((count as f64 / elapsed) * 10.0).round() / 10.0
            } else {
                0.0
            },
            latency_us_min: latencies.iter().min().copied(),
            latency_us_max: latencies.iter().max().copied(),
            latency_us_avg: mean(&latencies).map(|v| (v * 10.0).round() / 10.0),
            latency_us_p50: percentile(latencies.clone(), 50.0)
                .map(|v| (v * 10.0).round() / 10.0),
            latency_us_p99: percentile(latencies, 99.0)
                .map(|v| (v * 10.0).round() / 10.0),
            done_received,
            error: None,
        }
    }

    /// Run all 54 test combinations
    async fn run_all(&self) -> Vec<TestResult> {
        let total = PUB_QOS_VALUES.len()
            * DELAY_VALUES.len()
            * MSG_SIZE_VALUES.len()
            * SUB_QOS_VALUES.len();
        println!("Starting {total} tests ({TEST_DURATION_SECS}s each) …\n");

        let mut results = Vec::new();
        let mut test_num = 0usize;

        for &pub_qos in &PUB_QOS_VALUES {
            for &delay_ms in &DELAY_VALUES {
                for &msg_size in &MSG_SIZE_VALUES {
                    for &sub_qos in &SUB_QOS_VALUES {
                        test_num += 1;
                        let label = format!(
                            "pubQoS={pub_qos}  subQoS={sub_qos}  delay={delay_ms}ms  size={msg_size}B"
                        );
                        print!("[{test_num:>2}/{total}] {label} … ");
                        let _ = std::io::stdout().flush();

                        let result = self
                            .run_one_test(pub_qos, delay_ms, msg_size, sub_qos)
                            .await;

                        let status = format!(
                            "{} msgs  {}/s  ",
                            result.messages_received, result.throughput_msg_s
                        );
                        if let (Some(avg), Some(p99)) =
                            (result.latency_us_avg, result.latency_us_p99)
                        {
                            println!("{status}avg={avg}µs  p99={p99}µs");
                        } else {
                            println!("{status}");
                        }

                        results.push(result);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
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

    let host = broker_host();
    let port = broker_port();
    println!("Connecting to MQTT broker at {host}:{port}");

    let analyser = Analyser::new(host, port);
    let results = analyser.run_all().await;

    print_summary(&results);

    // Save JSON
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
