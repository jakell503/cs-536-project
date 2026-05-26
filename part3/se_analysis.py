import json
import time
from datetime import datetime, timedelta
from concurrent.futures import CancelledError
from google.cloud import pubsub_v1
import pandas as pd
from sqlalchemy import create_engine
from se_validations import validate_record
from transformations import state_plane_to_latlon, create_time_stamp

# ── Configuration ────────────────────────────────────────────────────────────
# GPC
PROJECT_ID = "gypsy-493704"
SUBSCRIPTION_ID = "se_analysis_sub"
# Database 
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = "myuser"   
DB_PASSWORD = "12345"
DB_CONN = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_NAME}"

# ── Function and Variables ────────────────────────────────────────────────────────────
streaming_pull = None

def load_pandas(df, connection_string=DB_CONN, table_name="StopEvent"):
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

def make_stats():
    return  {
        "start_time": datetime.now(),
        "received_count": 0,
        "valid_count": 0,
        "invalid_count": 0,
        "expected_messages": 0,
        "sentinel_ts": None,
        "unique_vehicle_ids": set(),
        "valid_records": [],
        "invalid_records": []
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
    print(f"  Unique Vehicles: {len(state['unique_vehicle_ids'])}")
    print(f"  Valid:    {state['valid_count']}")
    print(f"  Invalid:  {state['invalid_count']}")
    print(f"  Wall time: {wall_seconds:.2f} sec ({wall_hms})")
    print(f"  Throughput: {throughput:.2f} msg/s")
    if state["sentinel_ts"]:
        print(f"  Sentinel received at: {datetime.fromtimestamp(state['sentinel_ts']).strftime('%Y-%m-%d %H:%M:%S')}")

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
            valid_check = validate_record(payload) # Validate the record
            if valid_check == []:
                state["valid_records"].append(payload) # Store valid record for later use (dataframe)
                
                # Transform coordinates and timestamps for valid records
                # Coordinate transformation
                lat, lon = state_plane_to_latlon(payload["x_coordinate"], payload["y_coordinate"])
                payload["gps_latitude"] = lat
                payload["gps_longitude"] = lon
                
                # Timestamp transformation
                # Ensure service_date is a date/datetime before creating timestamps
                base_date = payload.get("service_date")
                if isinstance(base_date, str):
                    try:
                        base_date = datetime.fromisoformat(base_date)
                    except Exception:
                        try:
                            base_date = datetime.strptime(base_date, "%Y-%m-%d")
                        except Exception:
                            state["invalid_records"].append({"record": payload, "violations": ["A8: invalid service_date format"]})
                            state["invalid_count"] += 1
                            state["received_count"] += 1
                            return

                leave_time, stop_time, arrive_time = create_time_stamp(
                    payload["leave_time"],
                    payload["stop_time"],
                    payload["arrive_time"],
                    base_date
                )
                payload["leave_time"] = leave_time
                payload["stop_time"] = stop_time
                payload["arrive_time"] = arrive_time

                state["unique_vehicle_ids"].add(payload["vehicle_number"])
                state["valid_count"] += 1
                # Count this processed data message so the stop condition progresses
                state["received_count"] += 1
                #print(payload)
            else:
                state["invalid_records"].append({"record": payload, "violations": valid_check})
                state["unique_vehicle_ids"].add(payload["vehicle_number"])
                state["invalid_count"] += 1
                state["received_count"] += 1
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        state["invalid_count"] += 1
        # Bad/undecodable message still counts toward received messages
        state["received_count"] += 1
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
    ##### Process valid records and make a dataframe #####
    valid_df = pd.DataFrame(state["valid_records"])
    columns_to_keep = ['trip_id', 'vehicle_number', 'leave_time', 'train', 'route_number',
       'direction', 'service_key', 'trip_number', 'stop_time', 'arrive_time',
       'dwell','location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
       'maximum_speed', 'train_mileage', 'pattern_distance','location_distance', 
       'gps_latitude', 'gps_longitude','data_source', 'schedule_status']
    valid_df = valid_df[columns_to_keep]
    valid_df['service_key'] = valid_df['service_key'].astype(str).str[:1]

    loaded_count = load_pandas(valid_df, table_name="stopevent")
    print(f"Loaded {loaded_count} valid records into the database.")



if __name__== "__main__":
    main()
