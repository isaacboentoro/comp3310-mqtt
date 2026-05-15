"""
MQTT Performance Analyser
Runs 54 test combinations (3 pub QoS × 2 delays × 3 msg sizes × 3 sub QoS)
Each test: configures the Rust publisher, then collects counter messages for 30s.
"""

import paho.mqtt.client as mqtt
import time
import json
import statistics
import sys
from itertools import product

# ── parameters ──────────────────────────────────────────────────────────────
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TEST_DURATION = 30  # seconds per test
PUB_QOS_VALUES = [0, 1, 2]
DELAY_VALUES = [0, 100]        # ms
MSG_SIZE_VALUES = [1, 1000, 4000]
SUB_QOS_VALUES = [0, 1, 2]

# ── analyser ────────────────────────────────────────────────────────────────

class Analyser:
    def __init__(self):
        self.messages = []        # list of dicts: seq, sent_us, recv_us, latency_us
        self.done_received = False
        self.start_time = None
        self.test_active = False

    def _on_message(self, client, userdata, msg):
        topic = msg.topic

        # "done" signal from publisher
        if topic == "request/go":
            payload = msg.payload.decode().strip()
            if payload == "done":
                self.done_received = True
            return

        # counter message: "{seq}:{timestamp_us}:{padding}"
        if topic.startswith("counter/"):
            if not self.test_active:
                return
            try:
                now_us = time.time_ns() // 1000
                payload = msg.payload.decode(errors="replace")
                parts = payload.split(":", 2)
                if len(parts) >= 2:
                    seq = int(parts[0])
                    sent_us = int(parts[1])
                    self.messages.append({
                        "seq": seq,
                        "sent_us": sent_us,
                        "recv_us": now_us,
                        "latency_us": int(now_us - sent_us),
                    })
            except (ValueError, IndexError):
                pass  # malformed message, skip

    def run_one_test(self, pub_qos, delay_ms, msg_size, sub_qos) -> dict:
        """Connect, subscribe, configure publisher, collect 30s, disconnect."""

        client_id = f"analyser-{pub_qos}-{delay_ms}-{msg_size}-{sub_qos}"
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        client.on_message = self._on_message

        # ── connect ─────────────────────────────────────────────────────
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_start()

        # ── subscribe ────────────────────────────────────────────────────
        counter_topic = f"counter/{pub_qos}/{delay_ms}/{msg_size}"
        client.subscribe(counter_topic, qos=sub_qos)
        client.subscribe("request/go", qos=sub_qos)

        time.sleep(0.3)  # let subscriptions settle

        # ── configure publisher ──────────────────────────────────────────
        # Publisher listens on request/qos, request/delay, request/messagesize, request/go
        client.publish("request/qos", str(pub_qos), qos=1)
        client.publish("request/delay", str(delay_ms), qos=1)
        client.publish("request/messagesize", str(msg_size), qos=1)

        time.sleep(0.3)

        # ── run ──────────────────────────────────────────────────────────
        self.messages = []
        self.done_received = False
        self.test_active = True
        self.start_time = time.time()

        client.publish("request/go", "start", qos=1)

        # wait for 30s burst + grace period
        deadline = time.time() + TEST_DURATION + 10
        while time.time() < deadline and not self.done_received:
            time.sleep(0.2)

        self.test_active = False
        elapsed = time.time() - self.start_time

        # ── disconnect ───────────────────────────────────────────────────
        client.unsubscribe(counter_topic)
        client.unsubscribe("request/go")
        client.loop_stop()
        client.disconnect()

        # ── stats ────────────────────────────────────────────────────────
        latencies = [m["latency_us"] for m in self.messages]
        count = len(self.messages)

        if count == 0:
            print(f"  ⚠  WARNING: 0 messages received!", file=sys.stderr)

        return {
            "pub_qos": pub_qos,
            "sub_qos": sub_qos,
            "delay_ms": delay_ms,
            "msg_size": msg_size,
            "messages_received": count,
            "messages_expected": 0,  # filled after all tests
            "elapsed_s": round(elapsed, 2),
            "throughput_msg_s": round(count / elapsed, 1) if elapsed > 0 and count > 0 else 0.0,
            "latency_us_min": min(latencies) if latencies else None,
            "latency_us_max": max(latencies) if latencies else None,
            "latency_us_avg": round(statistics.mean(latencies), 1) if latencies else None,
            "latency_us_p50": round(statistics.median(latencies), 1) if latencies else None,
            "latency_us_p99": _percentile(latencies, 99) if latencies else None,
            "done_received": self.done_received,
        }

    def run_all(self) -> list[dict]:
        """Iterate over all 54 combinations and collect results."""
        total = len(PUB_QOS_VALUES) * len(DELAY_VALUES) * len(MSG_SIZE_VALUES) * len(SUB_QOS_VALUES)
        print(f"Starting {total} tests ({TEST_DURATION}s each) …\n")
        results = []

        for i, (pub_qos, delay_ms, msg_size, sub_qos) in enumerate(
            product(PUB_QOS_VALUES, DELAY_VALUES, MSG_SIZE_VALUES, SUB_QOS_VALUES), start=1
        ):
            label = f"pubQoS={pub_qos}  subQoS={sub_qos}  delay={delay_ms}ms  size={msg_size}B"
            print(f"[{i:>2}/{total}] {label} … ", end="", flush=True)

            try:
                r = self.run_one_test(pub_qos, delay_ms, msg_size, sub_qos)
                results.append(r)
                status = f"{r['messages_received']} msgs  {r['throughput_msg_s']}/s  "
                if r["latency_us_avg"] is not None:
                    status += f"avg={r['latency_us_avg']}µs  p99={r['latency_us_p99']}µs"
                print(status)
            except Exception as e:
                print(f"FAILED: {e}")
                results.append({
                    "pub_qos": pub_qos, "sub_qos": sub_qos,
                    "delay_ms": delay_ms, "msg_size": msg_size,
                    "error": str(e),
                })

            time.sleep(1)  # small gap between tests

        return results


# ── helpers ─────────────────────────────────────────────────────────────────

def _percentile(data: list[float], p: float) -> float:
    """Compute the p-th percentile (linear interpolation)."""
    if not data:
        return None
    sorted_data = sorted(data)
    n = len(sorted_data)
    rank = (p / 100.0) * (n - 1)
    lo = int(rank)
    hi = lo + 1
    if hi >= n:
        return sorted_data[-1]
    frac = rank - lo
    return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])


def print_summary(results: list[dict]):
    """Print a readable summary table."""
    print("\n" + "=" * 100)
    print(f"{'pubQ':>5} {'subQ':>5} {'delay':>6} {'size':>5}  "
          f"{'msgs':>7} {'msg/s':>8} {'avg(µs)':>10} {'p99(µs)':>10} {'min(µs)':>10} {'max(µs)':>10}")
    print("-" * 100)

    for r in results:
        if "error" in r:
            print(f"{r['pub_qos']:>5} {r['sub_qos']:>5} {r['delay_ms']:>5}ms {r['msg_size']:>4}B  "
                  f"  ERROR: {r['error']}")
        else:
            print(f"{r['pub_qos']:>5} {r['sub_qos']:>5} {r['delay_ms']:>5}ms {r['msg_size']:>4}B  "
                  f"{r['messages_received']:>7} {r['throughput_msg_s']:>7.0f}/s "
                  f"{r['latency_us_avg'] or 'N/A':>10} {r['latency_us_p99'] or 'N/A':>10} "
                  f"{r['latency_us_min'] or 'N/A':>10} {r['latency_us_max'] or 'N/A':>10}")

    print("=" * 100)


# ── main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    analyser = Analyser()
    try:
        results = analyser.run_all()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)

    print_summary(results)

    # save JSON for later analysis
    with open("results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\nSaved results to results.json")
