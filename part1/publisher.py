import json
import time
import requests
from google.cloud import pubsub_v1

#----------------Configuration----------------#
PROJECT_ID = "gypsy-493704"
PROJECT_NAME = "gypsy"
TOPIC_ID = "bc_topic"
INPUT_FILE = "VehicleIDs.csv"

#----------------Read Vehicle IDs from CSV----------------#
def read_vehicle_ids(file_path, PROJECT_NAME):
    vehicle_ids = []
    with open(file_path, 'r') as file:
        for line in file:
            vehicle_id = line.strip()
            if vehicle_id:
                vehicle_ids.append(vehicle_id)
    return vehicle_ids

# Get the list of vehicle IDs from the CSV file
vehicle_ids = read_vehicle_ids(INPUT_FILE, PROJECT_NAME)

print(f'Fetching data for {len(vehicle_ids)} vehicles...')
print('(This may take several minutes)\n')

#----------------Fetch BreadCrumb Data and Publish to Pub/Sub----------------#
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

failed_vehicles = []
received_count = 0
vehicles_with_data = set()
start_time = time.time()

for i, vid in enumerate(vehicle_ids):
    url = f'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}'
    try:
        response = requests.get(url, timeout = 10)
        response.raise_for_status()
        data = response.json()
        records = data if isinstance(data, list) else data.get('results', data)
        for record in records:
            record['vehicle_id'] = vid
            payload = json.dumps(record).encode('utf-8')
            publisher.publish(topic_path, payload)
            received_count += 1
        if records:
            vehicles_with_data.add(vid)
    except Exception as e:
        #print(f'Error fetching data for vehicle {vid}: {e}')
        failed_vehicles.append(vid)

    if (i + 1) % 25 == 0:
        print(f'  Progress: {i+1}/{len(vehicle_ids)} vehicles, '
              f'{received_count} records received so far...')

    time.sleep(0.1)

# Publish the sentinel message
sentinel_future = publisher.publish(
    topic_path,
    b'',
    message_type='SENTINEL',
    sentinel='PUBLISHING_COMPLETE',
)
sentinel_future.result(timeout=30)
sentinel_message_sent = time.time()
print('\nSentinel published to bc_topic (PUBLISHING_COMPLETE).')

elapsed_seconds = sentinel_message_sent - start_time
throughput = (received_count / elapsed_seconds) if elapsed_seconds > 0 else 0.0

print('Summary Statistics:')
print(f'BreadCrumb began accessing at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))}')
print(f'Unique vehicle IDs with data received: {len(vehicles_with_data)}')
print(f'Total bread crumbs published: {received_count}')
print(f'Sentinel message published at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sentinel_message_sent))}')
print(f'Elapsed wall-clock time: {elapsed_seconds:.2f} seconds')
print(f'Throughput (crumbs/sec): {throughput:.2f}')
