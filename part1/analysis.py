import json
import time
from datetime import datetime, timedelta
from concurrent.futures import CancelledError
from google.cloud import pubsub_v1


# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID = "gypsy-493704"
SUBSCRIPTION_ID = "analysis_sub"

# Handle missing times
def _fmt_ts(unix_ts):
    if unix_ts is None:
        return "N/A"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_ts))


def _fmt_breadcrumb_ts(breadcrumb_ts):
    if breadcrumb_ts is None:
        return "N/A"
    return breadcrumb_ts.strftime("%d%b%Y:%H:%M:%S").upper()


def _parse_breadcrumb_ts(payload):
    opd_date = payload.get("OPD_DATE")
    act_time = payload.get("ACT_TIME")
    if opd_date is None or act_time is None:
        return None

    try:
        base_date = datetime.strptime(str(opd_date), "%d%b%Y:%H:%M:%S")
        act_seconds = int(act_time) # Assuming ACT_TIME is in seconds.
    except (TypeError, ValueError):
        return None

    return base_date + timedelta(seconds=act_seconds)

# Statistics datastructure. It will reset each new day.
def run_one_day(day_number):
    state = {
        "day_start_ts": time.time(),
        "first_breadcrumb_ts": None,
        "sentinel_ts": None,
        "received_count": 0,
        "unique_vehicle_ids": set(),
        "unique_trips": set(),
        "earliest_breadcrumb": None,
        "latest_breadcrumb": None,
        "streaming_pull": None,
    }
    # Listen and processes messages until a sentinel message is received.
    def callback(message):
        # Check for the sentinel message.
        attrs = message.attributes or {}
        if attrs.get("message_type") == "SENTINEL": # Message_type='SENTINEL defined in publisher.py'
            state["sentinel_ts"] = time.time() # Time when the Sentinel is received. Goes in stats.
            print("Received sentinel message, publishing is complete for the day.")
            message.ack() # Acknowledge the message
            if state["streaming_pull"] is not None: # Cancel the streaming pull to stop receiving more messages.
                state["streaming_pull"].cancel() # Will cause .result() to raise CancelledError.
            return

        state["received_count"] += 1 # We are not at the Sentinel, so we can process the next message. Goes in stats.
        if state["first_breadcrumb_ts"] is None: # Check if this is the first breadcrumb and if so, record the time.
            state["first_breadcrumb_ts"] = time.time() # Goes in stats.
        try: # Load the message data as JSON.
            payload = json.loads(message.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError): # Bad message. Skip processing but acknowledge it to avoid redelivery.
            message.ack() # Acknowledge the message
            return
        # Capture all the stats that we want.
        if isinstance(payload, dict):

            vehicle_id = payload.get("VEHICLE_ID") # Returns None if VEHICLE_ID is not found.
            if vehicle_id is None:
                vehicle_id = payload.get("vehicle_id") # Try lowercase if the uppercase key is not found.
            if vehicle_id is not None: # Found the VID
                state["unique_vehicle_ids"].add(vehicle_id) # Goes in stats

            # Same logic as VEHICLE_ID
            trip_id = payload.get("EVENT_NO_TRIP")
            if trip_id is None:
                trip_id = payload.get("EVENT_TRIP")
            if trip_id is not None:
                state["unique_trips"].add(trip_id)

            # Determine the breadcrumb date and time so what we can get the earliest and latest crumb.
            breadcrumb_ts = _parse_breadcrumb_ts(payload)
            if breadcrumb_ts is not None:
                if ( # There is no earliest crumb yet or this crumb is earlier than the current earliest.
                    state["earliest_breadcrumb"] is None
                    or breadcrumb_ts < state["earliest_breadcrumb"]
                ):
                    state["earliest_breadcrumb"] = breadcrumb_ts # Update earliest crumb.
                if ( # There is no latest crumb yet or this crumb is later than the current latest.
                    state["latest_breadcrumb"] is None
                    or breadcrumb_ts > state["latest_breadcrumb"]
                ):
                    state["latest_breadcrumb"] = breadcrumb_ts # Update latest crumb.

        message.ack() # Acknowledge the message
        # Track the progess by printing a message every 50,000 messages received.
        if state["received_count"] % 50000 == 0:
            elapsed = time.time() - state["day_start_ts"]
            print(f"Received {state['received_count']} messages after {elapsed:.2f} seconds.")

    print(f"Listening on subscription: {SUBSCRIPTION_ID} (day {day_number})")

    # Start the streaming pull to listen for messages. Process each message until the sentinel is received and then stop for the day.
    with pubsub_v1.SubscriberClient() as subscriber: # subscriber client to connect to Pub/Sub.
        sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID) # Subscription path.
        state["streaming_pull"] = subscriber.subscribe(sub_path, callback=callback) # Start the streaming pull and save the future to state so we can cancel it when the sentinel is received.

        try:
            state["streaming_pull"].result() # If the sentinel is received this will raise CancelledError
        except CancelledError:
            # Expected when sentinel cancels the streaming pull.
            pass

    # Gather stats and print summary for the day.
    sentinel_ts = state["sentinel_ts"] or time.time() # Fallback incase something went wrong and we never got the sentinel.
    first_ts = state["first_breadcrumb_ts"]
    active_span = (sentinel_ts - first_ts) if first_ts is not None else 0.0

    print(f"\n--- Subscriber Summary ({SUBSCRIPTION_ID}) ---")
    print(f"First breadcrumb received at : {_fmt_ts(first_ts)}")

    earliest = state["earliest_breadcrumb"]
    latest = state["latest_breadcrumb"]
    if earliest is not None:
        print(f"Earliest breadcrumb ts       : {_fmt_breadcrumb_ts(earliest)}")
    else:
        print("Earliest breadcrumb ts       : N/A")
    if latest is not None:
        print(f"Latest breadcrumb ts         : {_fmt_breadcrumb_ts(latest)}")
    else:
        print("Latest breadcrumb ts         : N/A")

    print(f"Unique vehicle_id count      : {len(state['unique_vehicle_ids'])}")
    print(f"Unique trip count            : {len(state['unique_trips'])}")
    print(f"Total breadcrumbs received   : {state['received_count']}")
    print(f"Sentinel received at         : {_fmt_ts(state['sentinel_ts'])}")
    print(f"Active wall time (sec)       : {active_span:.3f}")
    if active_span > 0:
        print(f"Analysis throughput (msg/s)  : {state['received_count'] / active_span:.1f}")
    else:
        print("Analysis throughput (msg/s)  : N/A")
    print()


def main():
    day_number = 1
    while True:
        run_one_day(day_number)
        day_number += 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutting down subscriber.")
