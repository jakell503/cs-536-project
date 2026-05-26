# se_backup.py
import json
import time
import gzip
from datetime import datetime, timedelta
from concurrent.futures import CancelledError
from google.cloud import pubsub_v1
import os


# ── Configuration ────────────────────────────────────────────────────────────
#GPC
PROJECT_ID = "gypsy-493704"
SUBSCRIPTION_ID = "se_backup_sub"

# ── Function and Variables ────────────────────────────────────────────────────────────
streaming_pull = None
stopevents = []
backup_date = time.strftime("%Y%m%d-%H%M%S")
backup_file = f"se_{backup_date}.json.gz"

def make_stats():
    return  {
        "start_time": datetime.now(),
        "received_count": 0,
        "valid_count": 0,
        "expected_messages": 0,
        "sentinel_ts": None,
        "uncompressed_size_mb": 0.0,
        "compressed_size_mb": 0.0,
        "compressed_ts": None
    }

def print_stats(state):
    # end timestamp: sentinel if present, otherwise now
    end_ts = state["sentinel_ts"] or time.time()
    # start_time stored as datetime; convert to epoch seconds
    start_ts = state["start_time"].timestamp() if isinstance(state["start_time"], datetime) else float(state["start_time"])
    wall_seconds = max(0.0, end_ts - start_ts)
    wall_hms = str(timedelta(seconds=int(wall_seconds)))

    throughput = state["received_count"] / wall_seconds if wall_seconds > 0 else 0.0

    print(f"\nStats at {datetime.now().strftime('%Y-%m-%d %H:%M')}:")
    print(f"  Received: {state['received_count']}")
    print(f"  Wall time: {wall_seconds:.2f} sec ({wall_hms})")
    print(f"  Throughput: {throughput:.2f} msg/s")
    if state["sentinel_ts"]:
        print(f"  Sentinel received at: {datetime.fromtimestamp(state['sentinel_ts']).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Compressed back up file at: {datetime.fromtimestamp(state['compressed_ts']).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Size of back up file before compression: {state['uncompressed_size_mb']:.2f} MB)")
    print(f"  Size of back up file after compression: {state['compressed_size_mb']:.2f} MB)")
    
def callback(message, state):
    global streaming_pull
    try:
        raw_message = message.data.decode("utf-8")
        payload = json.loads(raw_message)
        # Sentinel branch
        if payload.get("sentinel") is True:
            state["expected_messages"] = int(payload["total_count"])
            state["sentinel_ts"] = time.time()
        # Normal record branch
        else:
            stopevents.append(payload)
            state["received_count"] += 1
    except (UnicodeDecodeError, json.JSONDecodeError): # Bad message. Skip processing but acknowledge it to avoid redelivery.
        pass
    finally:
        message.ack()
    # Check stop condition after processing any message
    if (
        state["sentinel_ts"] is not None
        and state["received_count"] >= state["expected_messages"]
    ):
        print("Received all expected data messages. Stopping.")
        if streaming_pull is not None:
            streaming_pull.cancel()
        json_bytes = json.dumps(stopevents).encode("utf-8")
        state["uncompressed_size_mb"] = len(json_bytes) / (1024 * 1024)

        with gzip.open(backup_file, "wb", compresslevel=6) as f:
            f.write(json_bytes)
        state["compressed_ts"] = time.time()
        state["compressed_size_mb"] = os.path.getsize(backup_file) / (1024 * 1024)

        print_stats(state)



def main():
    global streaming_pull
    state = make_stats()
    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
    print(f"\nListening for messages on {subscription_path} at {state['start_time'].strftime('%Y-%m-%d %H:%M')}...")
    ##### Start the streaming pull to listen for messages.  #####
    with pubsub_v1.SubscriberClient() as subscriber:
        streaming_pull = subscriber.subscribe(
            subscription_path,
            callback=lambda message: callback(message, state),
        )
        try:
            streaming_pull.result()
        except KeyboardInterrupt:
            streaming_pull.cancel()
            print("\n User stopped stream.")
        except CancelledError:
            pass

if __name__== "__main__":
    main()
