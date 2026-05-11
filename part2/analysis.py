import json
import time
from datetime import datetime, timedelta
from concurrent.futures import CancelledError
from google.cloud import pubsub_v1
import pandas as pd
from sqlalchemy import create_engine

try:
    from . import assertions
    from .invalid_records import write_invalid_records
except ImportError:
    import assertions
    from invalid_records import write_invalid_records

# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID = "gypsy-493704"
SUBSCRIPTION_ID = "analysis_sub"
 
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = "myuser"   
DB_PASSWORD = "12345"

DB_CONN = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_NAME}"


invalid_records = []
valid_records = []

# Convert the time stamp to human readable
def _fmt_ts(unix_ts):
    if unix_ts is None:
        return "N/A"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_ts))

# Convert the breadcrumb timestamp to human readable format. The breadcrumb timestamp is in the format of OPD_DATE + ACT_TIME (in seconds from midnight)
def _fmt_breadcrumb_ts(breadcrumb_ts):
    if breadcrumb_ts is None:
        return "N/A"
    return breadcrumb_ts.strftime("%d%b%Y:%H:%M:%S").upper()
# Parse the breadcrumb timestamp from the message payload. It is called to variable breadcrumb_ts but it is actually a datetime object 
# representing the date and time of the breadcrumb. It is calculated by taking the OPD_DATE and adding the ACT_TIME (in seconds) to it.
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

def load_pandas(df, connection_string=DB_CONN, table_name="breadcrumb"):
    """
    Load data using a pandas DataFrame and DataFrame.to_sql().

    Parameters
    ----------
    df : pandas.DataFrame
    connection_string : str
    table_name : str

    Returns
    -------
    int  -- number of rows loaded
    """
   
    try:
        engine = create_engine(connection_string)
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="append", index=False)
        return len(df)
    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        return 0


# Statistics datastructure. It will reset each new day.
def run_one_day():
    state = {
        "day_start_ts": time.time(),
        "first_breadcrumb_ts": None,
        "sentinel_ts": None,
        "total_processing_time": None,
        "received_count": 0,
        "expected_crumbs": None,
        "unique_vehicle_ids": set(),
        "unique_trips": set(),
        "earliest_breadcrumb": None,
        "latest_breadcrumb": None,
        "streaming_pull": None,
        
    }
    # Listen and processes messages until a sentinel message is received.
    def callback(message):
        ##### Check for the sentinel message #####
        attrs = message.attributes or {}
        if attrs.get("message_type") == "SENTINEL": 
            total_crumbs_str = attrs.get("total_crumbs")
            if total_crumbs_str:
                try:
                    state["expected_crumbs"] = int(total_crumbs_str)
                except ValueError:
                    print(f"Warning: invalid total_crumbs value: {total_crumbs_str}")
                    pass
            state["sentinel_ts"] = time.time() # Time when the Sentinel is received. Goes in stats.
            print(f"Received sentinel message. {state['expected_crumbs']} total crumbs sent for the day.")
            message.ack() # Acknowledge the message
            ##### Sentinel has been received and we check for all expected crumbs #####
            if (
                state["expected_crumbs"] is not None
                and state["received_count"] >= state["expected_crumbs"]
            ):
                print(
                    f"Already received all expected crumbs ({state['received_count']}). Stopping."
                )
                if state["streaming_pull"] is not None:
                    state["streaming_pull"].cancel()
            return
        ##### We are not at the sentinel so we continue processing #####
        state["received_count"] += 1 

        if state["first_breadcrumb_ts"] is None: # Check if this is the first breadcrumb and if so, record the time.
            state["first_breadcrumb_ts"] = time.time() # Goes in stats.
        ##### Load the JSON payload #####    
        try: 
            payload = json.loads(message.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError): # Bad message. Skip processing but acknowledge it to avoid redelivery.
            message.ack() # Acknowledge the message
            return

        ##### Validate the payload and separate valid and invalid records #####
        valid_check = assertions.validate_record(payload)
        enhanced_payload = {
                "violations": valid_check,
                "record": payload
            } 
        if valid_check == []:
            valid_records.append(enhanced_payload['record'])

        else:
            invalid_records.append(enhanced_payload)
                   
        ##### Capture all the stats that we want #####
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

        ##### After processing the message, check if we have received the sentinel and all expected crumbs. Stop if we have #####
        if state.get("expected_crumbs") is not None and state["received_count"] >= state["expected_crumbs"]:
            print(f"Received all expected crumbs ({state['received_count']}). Stopping for the day.")
            if state["streaming_pull"] is not None: # Cancel the streaming pull to stop receiving more messages.
                state["streaming_pull"].cancel() # Will cause .result() to raise CancelledError.
            return

    ##### Start the streaming pull to listen for messages. Process each message until the sentinel is received and then stop for the day #####
    with pubsub_v1.SubscriberClient() as subscriber: # subscriber client to connect to Pub/Sub.
        sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID) # Subscription path.
        state["streaming_pull"] = subscriber.subscribe(sub_path, callback=callback) # Start the streaming pull and save the future to state so we can cancel it when the sentinel is received.

        try:
            state["streaming_pull"].result() # If the sentinel is received and we have all the messages, this will raise CancelledError
        except CancelledError:
            # Expected when sentinel cancels the streaming pull.
            pass

     ##### write invalid breadcrumbs to a dated JSON file #####
    if invalid_records:
        write_invalid_records(invalid_records)

    ##### Process valid records and make a dataframe #####
    valid_df = pd.DataFrame(valid_records) # make a df from the list of JSON records
    
    valid_df['OPD_DATE'] = pd.to_datetime(valid_df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')
    valid_df['ACT_TIME'] = pd.to_timedelta(valid_df['ACT_TIME'], unit='s')
    valid_df['TIMESTAMP'] = valid_df['OPD_DATE'] + valid_df['ACT_TIME']

    # Create a SPEED field
    valid_df = valid_df.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP'], ascending=True)
    delta_seconds = valid_df.groupby('EVENT_NO_TRIP')['TIMESTAMP'].diff().dt.total_seconds()
    delta_meters = valid_df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    valid_df['SPEED'] = (delta_meters / delta_seconds).fillna(0) 

    # Drop columns that we no longer require
    columns_to_drop = ["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP", "ACT_TIME", "OPD_DATE"]
    valid_df = valid_df.drop(columns = columns_to_drop)
    # Rename fields
    column_mapping = {
    "EVENT_NO_TRIP": "trip_id",
    # "VEHICLE_ID": "vehicle_id",
    "TIMESTAMP": "timestamp",
    "GPS_LATITUDE": "latitude",
    "GPS_LONGITUDE": "longitude",
    "SPEED": "speed",
    "METERS": "meters"
    
}
    valid_df = valid_df.rename(columns=column_mapping)
    column_order = ["trip_id", "vehicle_id", "timestamp", "latitude", "longitude", "speed", "meters"]
    valid_df = valid_df[column_order]

    loaded_count = load_pandas(valid_df, table_name="breadcrumb")
    print(f"Loaded {loaded_count} valid records into the database.")
    state["total_processing_time"] = time.time()



   
    ####################### Gather stats and print summary for the day #######################
    total_processing_time = state["total_processing_time"] or time.time() # Fallback incase something went wrong and we never got the sentinel.
    first_ts = state["first_breadcrumb_ts"]
    active_span = (total_processing_time - first_ts) if first_ts is not None else 0.0

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
    print(f"Total processing time (sec)  : {total_processing_time - first_ts:.3f}")
    print(f"Active wall time (sec)       : {active_span:.3f}")
    if active_span > 0:
        print(f"Analysis throughput (msg/s)  : {state['received_count'] / active_span:.1f}")
    else:
        print("Analysis throughput (msg/s)  : N/A")
    print()
    print("Assertion Summary:")
    print(f"Valid records: {len(valid_records)}")
    print(f"Invalid records: {len(invalid_records)}")

    # Return True if a sentinel was received for this day (so caller can stop listening).
    return state["sentinel_ts"] is not None

# 
def main():
    # Run a single analysis pass and return whether a sentinel was received.
    return run_one_day()

# I'm trying to make it so that it will only run once and then exit unless there is an error. 
# I can't seem to figure it out so I will leaver fore now. This is so I do not have to reboot
# when I make changes to the code.
# I change Restart=Always to Restart=on-failure. Revisit if I have time. 
if __name__ == "__main__":
    import sys, traceback
    try:
        success = main()
        if success:
            print("Completed successfully — sentinel received. Exiting with code 0.\n")
            sys.exit(0)
        else:
            print("Completed without sentinel. Exiting with code 0.\n")
            sys.exit(0)
    except KeyboardInterrupt:
        print("Shutting down subscriber.\n")
        sys.exit(0)
    except Exception:
        print("Unhandled exception in analysis.py — exiting with code 1.\n")
        traceback.print_exc()
        sys.exit(1)
