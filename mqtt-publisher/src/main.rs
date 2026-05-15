use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::Instant;

// Config

#[derive(Debug, Clone)]
struct RunConfig {
    qos: u8,
    delay_ms: u64,
    message_size: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig { qos: 0, delay_ms:0, message_size :1 }
    }
}

fn to_qos(n: u8) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

//Microseconds since unix epoch
fn now_us() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH)
    .expect("system clock before epoch").as_micros()
}

// entry point
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // connect to local broker
    let mut opts = MqttOptions::new("rust-publisher", "localhost", 1883);
    opts.set_keep_alive(Duration::from_secs(30));

    // Large channel to avoid blocking waiting for eventloop
    // (max 2^16-1)
    let (client, mut eventloop) = AsyncClient::new(opts, 65_535);

    // spawn eventloop
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<(String, String)>();

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).into_owned();
                    let _ = msg_tx.send((p.topic, payload));
                }
                Ok(_) => { /*connack, suback, etc */}
                Err(e) => {
                    eprintln!("[eventloop] error: {e} - retrying...");
                    tokio::time::sleep(Duration::from_secs(1)).await; // dont flood

                }
            }
        }
    });

    // topics

    for topic in ["request/qos", "request/delay", "request/messagesize", "request/go"] {
        client.subscribe(topic, QoS::AtLeastOnce).await?;
    }

    println!("[publisher] ready, waiting for request/# configuration...");

    // main eventloop (listen, configure, publish, repeat)

    let mut cfg = RunConfig::default();
    loop {
        let (topic, payload) = match msg_rx.recv().await {
            Some(m) => m,
            None => break,
        };

        match topic.as_str() {
            "request/qos" => {
                cfg.qos = payload.trim().parse::<u8>().unwrap_or(0).min(2);
                println!("[config] qos = {}", cfg.qos);
            }

            "request/delay" => {
                cfg.delay_ms = payload.trim().parse::<u64>().unwrap_or(0);
                println!("[config] delay_ms = {}", cfg.delay_ms);
            }

            "request/messagesize" => {
                cfg.message_size = payload.trim().parse::<usize>().unwrap_or(1);
                println!("[config] message_size = {}" , cfg.message_size);
            }

            // start, 30s burst then signal done 

            "request/go" if payload.trim() == "start" => {
                run_publish_burst(&client, &cfg).await?;

                //signal to analyzer 
                client.publish("request/go", QoS::AtLeastOnce, false, "done").await?;

                println!("[publisher] sent 'done', back to listening...\n");
            }

            "request/go" => {}

            other => {
                eprintln!("[publisher] unexpected topic: {other}");
            }
        }
    }

    Ok(())
}

async fn run_publish_burst(
    client: &AsyncClient,
    cfg:&RunConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let pub_topic = format!("counter/{}/{}/{}", cfg.qos, cfg.delay_ms, cfg.message_size);
    let qos = to_qos(cfg.qos);
    let padding = "x".repeat(cfg.message_size);
    let delay = (cfg.delay_ms > 0).then(|| Duration::from_millis(cfg.delay_ms));

    println!(
        "[publish] starting 30s burst, topic='{}' | qos = {} | delay ={:?} | size= {}",
        pub_topic, cfg.qos, delay, cfg.message_size
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut counter: u64 = 0;

    while Instant::now() < deadline {
        let ts = now_us();
        let message = format!("{counter}:{ts}:{padding}");

        client.publish(&pub_topic, qos, false, message).await?;
        counter += 1;

        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
    }

    println!("[publish] finished, sent {counter} messages");
    Ok(())
}
