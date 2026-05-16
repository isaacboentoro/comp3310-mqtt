use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, sleep_until, timeout, Instant};

// ── .env loader ────────────────────────────────────────────────────────────

fn load_dotenv() {
    let _ = dotenvy::from_filename("../.env");
    let _ = dotenvy::dotenv();
}

// ── constants ──────────────────────────────────────────────────────────────

const TEST_DURATION_SECS: u64 = 30;
const GRACE_PERIOD_SECS: u64 = 5;
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
        .expect("clock before epoch")
        .as_micros()
}

// ── CLI filters ────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct Filters {
    pub_qos:  Option<Vec<u8>>,
    sub_qos:  Option<Vec<u8>>,
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
                    f.pub_qos = Some(Self::parse_list(&args, &mut i, "--pub-qos",
                        |s| s.parse::<u8>().ok().filter(|&v| v <= 2)));
                }
                "--sub-qos" => {
                    f.sub_qos = Some(Self::parse_list(&args, &mut i, "--sub-qos",
                        |s| s.parse::<u8>().ok().filter(|&v| v <= 2)));
                }
                "--delay" => {
                    f.delay_ms = Some(Self::parse_list(&args, &mut i, "--delay",
                        |s| s.parse::<u64>().ok()));
                }
                "--size" => {
                    f.msg_size = Some(Self::parse_list(&args, &mut i, "--size",
                        |s| s.parse::<usize>().ok()));
                }
                "--help" | "-h" => {
                    print!(concat!(
                        "Usage: mqtt-analyser [OPTIONS]\n",
                        "\nOptions:\n",
                        "  --pub-qos <0,1,2>    Publisher QoS values to test (default: all)\n",
                        "  --sub-qos <0,1,2>    Subscriber QoS values to test (default: all)\n",
                        "  --delay   <ms,...>   Publish delay in ms (default: all)\n",
                        "  --size    <b,...>    Message sizes in bytes (default: all)\n",
                        "  -h, --help           Show this help\n",
                        "\nExamples:\n",
                        "  mqtt-analyser --pub-qos 0 --sub-qos 0 --delay 100 --size 1\n",
                        "  mqtt-analyser --delay 0 --size 1\n",
                    ));
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
        self.pub_qos .as_ref().map_or(true, |v| v.contains(&pub_qos))
            && self.sub_qos .as_ref().map_or(true, |v| v.contains(&sub_qos))
            && self.delay_ms.as_ref().map_or(true, |v| v.contains(&delay_ms))
            && self.msg_size.as_ref().map_or(true, |v| v.contains(&msg_size))
    }
}

// ── raw message ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct RawMsg {
    seq:     u64,
    sent_us: u128,
    recv_us: u128,
}

fn parse_payload(payload: &[u8]) -> Option<RawMsg> {
    let recv_us = now_us();
    // format: {seq}:{sent_us}:{padding…}
    let c1 = payload.iter().position(|&b| b == b':')?;
    let seq = std::str::from_utf8(&payload[..c1]).ok()?.parse::<u64>().ok()?;
    let rest = &payload[c1 + 1..];
    let c2 = rest.iter().position(|&b| b == b':')?;
    let sent_us = std::str::from_utf8(&rest[..c2]).ok()?.parse::<u128>().ok()?;
    Some(RawMsg { seq, sent_us, recv_us })
}

// ── $SYS snapshot ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
struct SysSnapshot {
    captured_at_s: u64,
    #[serde(skip_serializing_if = "Option::is_none")] bytes_received:               Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] bytes_sent:                   Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] messages_received:            Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] messages_sent:                Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] publish_messages_received:    Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] publish_messages_sent:        Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] publish_messages_dropped:     Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] clients_connected:            Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] clients_maximum:              Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] heap_current:                 Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] heap_maximum:                 Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] load_messages_received_1min:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")] load_messages_sent_1min:      Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")] store_messages_count:         Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] store_messages_bytes:         Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] subscriptions_count:          Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] uptime_s:                     Option<u64>,
}

impl SysSnapshot {
    fn apply(&mut self, topic: &str, value: &str) {
        let key = topic.strip_prefix("$SYS/broker/").unwrap_or(topic);
        let u = || value.parse::<u64>().ok();
        let f = || value.parse::<f64>().ok();
        match key {
            "bytes/received"                  => self.bytes_received               = u(),
            "bytes/sent"                      => self.bytes_sent                   = u(),
            "messages/received"               => self.messages_received            = u(),
            "messages/sent"                   => self.messages_sent                = u(),
            "publish/messages/received"       => self.publish_messages_received    = u(),
            "publish/messages/sent"           => self.publish_messages_sent        = u(),
            "publish/messages/dropped"        => self.publish_messages_dropped     = u(),
            "clients/connected"               => self.clients_connected            = u(),
            "clients/maximum"                 => self.clients_maximum              = u(),
            "heap/current"                    => self.heap_current                 = u(),
            "heap/maximum"                    => self.heap_maximum                 = u(),
            "load/messages/received/1min"     => self.load_messages_received_1min  = f(),
            "load/messages/sent/1min"         => self.load_messages_sent_1min      = f(),
            "store/messages/count"            => self.store_messages_count         = u(),
            "store/messages/bytes"            => self.store_messages_bytes         = u(),
            "subscriptions/count"             => self.subscriptions_count          = u(),
            "uptime" => {
                // Mosquitto sends "N seconds" or plain "N"
                self.uptime_s = value.split_whitespace().next()
                    .and_then(|s| s.parse::<u64>().ok());
            }
            _ => {}
        }
        self.captured_at_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

// ── per-test result ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct TestResult {
    pub_qos:  u8,
    sub_qos:  u8,
    delay_ms: u64,
    msg_size: usize,

    // throughput
    elapsed_s:         f64,
    messages_received: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_expected: Option<usize>,
    throughput_msg_s:  f64,

    // correctness
    loss_pct:         f64,
    out_of_order_pct: f64,
    duplicate_pct:    f64,

    // inter-message gap (consecutive seq pairs only, using sent_us)
    #[serde(skip_serializing_if = "Option::is_none")] gap_mean_ms:   Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")] gap_stddev_ms: Option<f64>,

    // one-way latency
    #[serde(skip_serializing_if = "Option::is_none")] latency_us_min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")] latency_us_max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")] latency_us_avg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")] latency_us_p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")] latency_us_p99: Option<f64>,

    // $SYS correlation
    #[serde(skip_serializing_if = "Option::is_none")] sys_before: Option<SysSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")] sys_after:  Option<SysSnapshot>,

    done_received: bool,
}

// ── statistics ─────────────────────────────────────────────────────────────

fn percentiles_p50_p99(mut data: Vec<i64>) -> (Option<f64>, Option<f64>) {
    if data.is_empty() { return (None, None); }
    data.sort_unstable();
    let n = data.len();
    let interp = |p: f64| -> f64 {
        let rank = (p / 100.0) * (n as f64 - 1.0);
        let lo   = rank as usize;
        let hi   = (lo + 1).min(n - 1);
        data[lo] as f64 + (rank - lo as f64) * (data[hi] as f64 - data[lo] as f64)
    };
    (Some((interp(50.0) * 10.0).round() / 10.0),
     Some((interp(99.0) * 10.0).round() / 10.0))
}

fn compute_stats(msgs: &[RawMsg]) -> (
    f64,         // loss_pct
    f64,         // out_of_order_pct
    f64,         // duplicate_pct
    Option<f64>, // gap_mean_ms
    Option<f64>, // gap_stddev_ms
    Option<i64>, // lat_min
    Option<i64>, // lat_max
    Option<f64>, // lat_avg
    Option<f64>, // lat_p50
    Option<f64>, // lat_p99
) {
    let n = msgs.len();
    if n == 0 {
        return (0.0, 0.0, 0.0, None, None, None, None, None, None, None);
    }

    // ── loss / ordering / duplicates ──────────────────────────────────────
    let mut seen:         HashSet<u64> = HashSet::with_capacity(n);
    let mut max_seq:      u64 = 0;
    let mut out_of_order: usize = 0;
    let mut duplicates:   usize = 0;

    for m in msgs {
        if !seen.insert(m.seq) {
            duplicates += 1;
        } else {
            if m.seq < max_seq { out_of_order += 1; }
            else { max_seq = m.seq; }
        }
    }

    let expected_unique = (max_seq + 1) as usize;
    let lost            = expected_unique.saturating_sub(seen.len());
    let loss_pct         = lost          as f64 / expected_unique as f64 * 100.0;
    let out_of_order_pct = out_of_order  as f64 / n               as f64 * 100.0;
    let duplicate_pct    = duplicates    as f64 / n               as f64 * 100.0;

    // ── inter-message gap (consecutive seq, sorted by seq, using sent_us) ─
    let mut by_seq: Vec<&RawMsg> = msgs.iter().collect();
    by_seq.sort_unstable_by_key(|m| m.seq);

    let gaps_ms: Vec<f64> = by_seq.windows(2)
        .filter(|w| w[1].seq == w[0].seq + 1)
        .map(|w| (w[1].sent_us as i128 - w[0].sent_us as i128) as f64 / 1000.0)
        .collect();

    let gap_mean_ms = if gaps_ms.is_empty() { None } else {
        let m = gaps_ms.iter().sum::<f64>() / gaps_ms.len() as f64;
        Some((m * 1000.0).round() / 1000.0)
    };
    let gap_stddev_ms = gap_mean_ms.map(|mean| {
        let var = gaps_ms.iter().map(|&g| (g - mean).powi(2)).sum::<f64>()
                  / gaps_ms.len() as f64;
        (var.sqrt() * 1000.0).round() / 1000.0
    });

    // ── latency ───────────────────────────────────────────────────────────
    let latencies: Vec<i64> = msgs.iter()
        .map(|m| m.recv_us as i64 - m.sent_us as i64)
        .collect();
    let lat_min = latencies.iter().min().copied();
    let lat_max = latencies.iter().max().copied();
    let lat_avg = {
        let s: i64 = latencies.iter().sum();
        Some((s as f64 / n as f64 * 10.0).round() / 10.0)
    };
    let (lat_p50, lat_p99) = percentiles_p50_p99(latencies);

    (
        (loss_pct         * 100.0).round() / 100.0,
        (out_of_order_pct * 100.0).round() / 100.0,
        (duplicate_pct    * 100.0).round() / 100.0,
        gap_mean_ms, gap_stddev_ms,
        lat_min, lat_max, lat_avg, lat_p50, lat_p99,
    )
}

// ── $SYS monitor ──────────────────────────────────────────────────────────

fn spawn_sys_monitor(
    host:   String,
    port:   u16,
    shared: Arc<Mutex<SysSnapshot>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut opts = MqttOptions::new("analyser-sys", &host, port);
            opts.set_keep_alive(Duration::from_secs(30));
            opts.set_clean_session(true);
            let (client, mut ev) = AsyncClient::new(opts, 256);
            let _ = client.subscribe("$SYS/#", QoS::AtMostOnce).await;

            loop {
                match ev.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        if let Ok(val) = std::str::from_utf8(&p.payload) {
                            shared.lock().unwrap().apply(&p.topic, val.trim());
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[sys] {e} — reconnecting in 5s");
                        break;
                    }
                }
            }
            drop(client);
            sleep(Duration::from_secs(5)).await;
        }
    })
}

// ── analyser ───────────────────────────────────────────────────────────────

struct Analyser {
    host: String,
    port: u16,
    sys:  Arc<Mutex<SysSnapshot>>,
}

impl Analyser {
    fn new(host: String, port: u16, sys: Arc<Mutex<SysSnapshot>>) -> Self {
        Self { host, port, sys }
    }

    fn sys_snapshot(&self) -> SysSnapshot {
        self.sys.lock().unwrap().clone()
    }

    async fn run_one_test(
        &self,
        pub_qos:  u8,
        delay_ms: u64,
        msg_size: usize,
        sub_qos:  u8,
    ) -> TestResult {
        let client_id = format!("analyser-{pub_qos}-{delay_ms}-{msg_size}-{sub_qos}");
        let mut opts = MqttOptions::new(&client_id, &self.host, self.port);
        opts.set_keep_alive(Duration::from_secs(60));
        opts.set_clean_session(true);
        let (client, mut eventloop) = AsyncClient::new(opts, 65_535);

        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Vec<RawMsg>>();
        let done_flag    = Arc::new(AtomicBool::new(false));
        let done_flag_ev = done_flag.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);

        // ── event loop task ──────────────────────────────────────────────
        let ev_handle = tokio::spawn(async move {
            const BATCH: usize = 4096;
            let mut batch: Vec<RawMsg> = Vec::with_capacity(BATCH);
            let mut ready_tx    = Some(ready_tx);
            let mut subacks_left: u8 = 2;

            loop {
                tokio::select! {
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(Packet::SubAck(_))) => {
                                subacks_left = subacks_left.saturating_sub(1);
                                if subacks_left == 0 {
                                    if let Some(tx) = ready_tx.take() { let _ = tx.send(()); }
                                }
                            }
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                if p.topic.as_bytes() == b"request/go" {
                                    if p.payload.as_ref() == b"done" {
                                        done_flag_ev.store(true, Ordering::Release);
                                    }
                                    continue;
                                }
                                if p.topic.as_bytes().starts_with(b"counter/") {
                                    if let Some(msg) = parse_payload(&p.payload) {
                                        batch.push(msg);
                                        if batch.len() >= BATCH {
                                            let full = std::mem::replace(&mut batch, Vec::with_capacity(BATCH));
                                            if batch_tx.send(full).is_err() { return; }
                                        }
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => { eprintln!("[ev] {e}"); break; }
                        }
                    }
                    _ = stop_rx.changed() => {
                        break;
                    }
                }
            }
            if !batch.is_empty() { let _ = batch_tx.send(batch); }
        });

        // ── subscribe and wait for confirmation ──────────────────────────
        let counter_topic = format!("counter/{pub_qos}/{delay_ms}/{msg_size}");
        let _ = client.subscribe(&counter_topic, to_qos(sub_qos)).await;
        let _ = client.subscribe("request/go",   to_qos(sub_qos)).await;

        if timeout(Duration::from_secs(5), ready_rx).await.is_err() {
            eprintln!("  ⚠  SubAck timeout");
        }

        // Stop previous publisher, then configure the new test
        let _ = client.publish("request/go", QoS::AtLeastOnce, false, "stop").await;
        sleep(Duration::from_millis(200)).await;

        let _ = client.publish("request/qos",        QoS::AtLeastOnce, false, pub_qos.to_string()).await;
        let _ = client.publish("request/delay",       QoS::AtLeastOnce, false, delay_ms.to_string()).await;
        let _ = client.publish("request/messagesize", QoS::AtLeastOnce, false, msg_size.to_string()).await;

        // Wait for config to reach the remote publisher (covers QoS-2 RTT)
        sleep(Duration::from_millis(500)).await;

        // ── snapshot $SYS before start ───────────────────────────────────
        let sys_before = self.sys_snapshot();

        // ── start the publisher ──────────────────────────────────────────
        let start = Instant::now();
        let _ = client.publish("request/go", QoS::AtLeastOnce, false, "start").await;

        let collect_end = start + Duration::from_secs(TEST_DURATION_SECS + GRACE_PERIOD_SECS);
        let cap = if delay_ms == 0 { 12_000_000 }
                  else { ((TEST_DURATION_SECS * 1000 / delay_ms) as usize + 64).next_power_of_two() };
        let mut messages: Vec<RawMsg> = Vec::with_capacity(cap);

        loop {
            tokio::select! {
                mb = batch_rx.recv() => match mb {
                    Some(b) => messages.extend(b),
                    None    => break,
                },
                _ = sleep_until(collect_end) => break,
            }
        }
        while let Ok(b) = batch_rx.try_recv() { messages.extend(b); }

        let elapsed      = start.elapsed().as_secs_f64();
        let done_received = done_flag.load(Ordering::Acquire);

        // ── snapshot $SYS after ──────────────────────────────────────────
        let sys_after = self.sys_snapshot();

        // ── teardown ─────────────────────────────────────────────────────
        let _ = client.unsubscribe(&counter_topic).await;
        let _ = client.unsubscribe("request/go").await;
        // Signal event loop to flush and exit
        let _ = stop_tx.send(true);
        // Wait for event loop to finish flushing
        sleep(Duration::from_millis(500)).await;
        while let Ok(b) = batch_rx.try_recv() { messages.extend(b); }
        drop(client);
        ev_handle.abort();

        // ── compute stats ─────────────────────────────────────────────────
        let n = messages.len();
        if n == 0 { eprintln!("  ⚠  0 messages received!"); }

        let (loss_pct, out_of_order_pct, duplicate_pct,
             gap_mean_ms, gap_stddev_ms,
             lat_min, lat_max, lat_avg, lat_p50, lat_p99) = compute_stats(&messages);

        TestResult {
            pub_qos, sub_qos, delay_ms, msg_size,
            elapsed_s: (elapsed * 100.0).round() / 100.0,
            messages_received: n,
            messages_expected: if delay_ms == 0 { None }
                else { Some((TEST_DURATION_SECS * 1000 / delay_ms) as usize) },
            throughput_msg_s: if elapsed > 0.0 && n > 0 {
                ((n as f64 / elapsed) * 10.0).round() / 10.0
            } else { 0.0 },
            loss_pct, out_of_order_pct, duplicate_pct,
            gap_mean_ms, gap_stddev_ms,
            latency_us_min: lat_min,
            latency_us_max: lat_max,
            latency_us_avg: lat_avg,
            latency_us_p50: lat_p50,
            latency_us_p99: lat_p99,
            sys_before: Some(sys_before),
            sys_after:  Some(sys_after),
            done_received,
        }
    }

    async fn run_all(&self, filters: &Filters) -> Vec<TestResult> {
        let matrix: Vec<(u8, u64, usize, u8)> = PUB_QOS_VALUES.iter()
            .flat_map(|&pq| DELAY_VALUES.iter()
                .flat_map(move |&d| MSG_SIZE_VALUES.iter()
                    .flat_map(move |&sz| SUB_QOS_VALUES.iter()
                        .map(move |&sq| (pq, d, sz, sq)))))
            .filter(|&(pq, d, sz, sq)| filters.matches(pq, sq, d, sz))
            .collect();

        let total = matrix.len();
        if total == 0 { eprintln!("No tests match the specified filters."); return vec![]; }

        let w = total.to_string().len();
        println!("Starting {total} test(s) ({TEST_DURATION_SECS}s each) …\n");

        let mut results = Vec::with_capacity(total);

        for (idx, (pub_qos, delay_ms, msg_size, sub_qos)) in matrix.into_iter().enumerate() {
            print!(
                "[{num:>width$}/{total}] pubQoS={pub_qos}  subQoS={sub_qos}  delay={delay_ms}ms  size={msg_size}B … ",
                num = idx + 1, width = w,
            );
            let _ = std::io::stdout().flush();

            let r = self.run_one_test(pub_qos, delay_ms, msg_size, sub_qos).await;

            println!(
                "{} msgs  {:.0}/s  loss={:.2}%  ooo={:.2}%  dup={:.2}%  \
                 gap={}/{}ms  lat_avg={:?}µs",
                r.messages_received, r.throughput_msg_s,
                r.loss_pct, r.out_of_order_pct, r.duplicate_pct,
                r.gap_mean_ms  .map_or("N/A".into(), |v| format!("{v:.3}")),
                r.gap_stddev_ms.map_or("N/A".into(), |v| format!("{v:.3}")),
                r.latency_us_avg,
            );

            results.push(r);
            if idx + 1 < total { sleep(Duration::from_secs(1)).await; }
        }

        results
    }
}

// ── summary table ──────────────────────────────────────────────────────────

fn print_summary(results: &[TestResult]) {
    let sep = "=".repeat(126);
    println!("\n{sep}");
    println!(
        "{:>4} {:>4} {:>6} {:>5}  {:>7} {:>8}  {:>7} {:>7} {:>7}  {:>10} {:>10}  {:>9} {:>9}",
        "pQoS","sQoS","delay","size",
        "msgs","msg/s",
        "loss%","ooo%","dup%",
        "gap_mean","gap_std",
        "avg_lat","p99_lat",
    );
    println!("{}", "-".repeat(126));
    for r in results {
        println!(
            "{:>4} {:>4} {:>5}ms {:>4}B  {:>7} {:>7.0}/s  {:>6.2}% {:>6.2}% {:>6.2}%  {:>9} {:>9}  {:>8} {:>8}",
            r.pub_qos, r.sub_qos, r.delay_ms, r.msg_size,
            r.messages_received, r.throughput_msg_s,
            r.loss_pct, r.out_of_order_pct, r.duplicate_pct,
            r.gap_mean_ms  .map_or("    N/A".into(), |v| format!("{v:>7.2}ms")),
            r.gap_stddev_ms.map_or("    N/A".into(), |v| format!("{v:>7.2}ms")),
            r.latency_us_avg.map_or("   N/A".into(), |v| format!("{v:>6.0}µs")),
            r.latency_us_p99.map_or("   N/A".into(), |v| format!("{v:>6.0}µs")),
        );
    }
    println!("{sep}");
}

// ── main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    load_dotenv();

    let filters = Filters::from_args();
    let host    = broker_host();
    let port    = broker_port();

    println!("Connecting to MQTT broker at {host}:{port}");

    let sys_shared  = Arc::new(Mutex::new(SysSnapshot::default()));
    let _sys_handle = spawn_sys_monitor(host.clone(), port, sys_shared.clone());

    // Let the $SYS monitor receive its first metrics before tests start
    sleep(Duration::from_secs(2)).await;

    let analyser = Analyser::new(host, port, sys_shared);
    let results  = analyser.run_all(&filters).await;

    print_summary(&results);

    match serde_json::to_string_pretty(&results) {
        Ok(json) => match std::fs::write("results.json", &json) {
            Ok(_)  => println!("\nResults saved to results.json"),
            Err(e) => eprintln!("Failed to write results.json: {e}"),
        },
        Err(e) => eprintln!("Failed to serialise results: {e}"),
    }
}