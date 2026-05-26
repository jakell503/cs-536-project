# se_publisher.py
import json
import time
import requests
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup

# ----------------Configuration----------------#
PROJECT_ID = "gypsy-493704"
PROJECT_NAME = "gypsy"
TOPIC_ID = "se_topic"
INPUT_FILE = "VehicleIDs.csv"
# INPUT_FILE = "VehicleIDsTest.csv"


# ----------------Stats----------------#
published = 0
not_published = 0
vehicle_ids_with_data = set()
start_time = time.time()


# ----------------Function to read Vehicle IDs from CSV----------------#
def read_vehicle_ids(file_path):
    """Read a csv of the vehicle ID's and store in a list.

    Args:
        file_path (csv): Path where the vehicle ID's live

    Returns:
        list: List of all the assigned vehicle ID's
    """
    vehicle_ids = []
    with open(file_path, 'r') as file:
        for line in file:
            vehicle_id = line.strip()
            if vehicle_id:
                vehicle_ids.append(vehicle_id)
    return vehicle_ids

# ----------------Function to parse html----------------#
def parse_html(soup):
    """Searches a table in html and returns the data in a list. Each element in
    the list represents a dict with one row of data.

    Args:
        soup (Object): An instance of BeautifulSoup

    Returns:
        List: A list of dictionaries.
    """
    # Extract service_date from the heading
    h1_tag = soup.find("h1")
    service_date = None
    if h1_tag:
        text = h1_tag.text.strip()
        # Extract date from "Trimet CAD/AVL stop data for YYYY-MM-DD"
        if "for " in text:
            service_date = text.split("for ")[-1]
    
    table = soup.find("table") # get the table
    # get the headers
    headers = []
    for th in table.find_all('th'):
        headers.append(th.text.strip())
      
    # get the rows
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        # skip empty
        if not cells:
            continue

        row_data = {}

        for i, td in enumerate(cells):
            row_data[headers[i]] = td.text.strip()
        
        # Add service_date to each row
        if service_date:
            row_data['service_date'] = service_date
        
        rows.append(row_data)
    return rows
     
# ----------------Fetch BreadCrumb Data and Publish to Pub/Sub----------------#
publisher = pubsub_v1.PublisherClient() # Initialize the Pub/Sub publisher client
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Get the list of vehicle IDs from the CSV file
vehicle_ids = read_vehicle_ids(INPUT_FILE)

publish_futures = []

print(f"Fetching data for {len(vehicle_ids)} vehicles at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}...")

for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
    try:
        response = requests.get(url, timeout = 10)
        response.raise_for_status() # Raise an exception for HTTP error

        soup = BeautifulSoup(response.content, features="html.parser")
        rows = parse_html(soup) # this is a list
        if rows: # if non empty then append
            vehicle_ids_with_data.add(vehicle_id)
        # Iterate through each row in the list of rows and publish as a JSON
        for row in rows:
            payload = json.dumps(row, indent=2).encode('utf-8')
            future = publisher.publish(topic_path, payload)
            publish_futures.append(future)
            published += 1
            #print(payload.decode('utf-8')) # Print the payload for debugging

    except requests.RequestException as e:
        not_published +=1
        #print(f"{vehicle_id}: request failed -> {e}")
    except Exception as e:
        not_published += 1
        #print(f"{vehicle_id}: parse failed -> {e}")
    time.sleep(0.1)

print(f"\nFinished fetching data for {len(vehicle_ids)} vehicles at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.")
# Send the sentinel JSON with the count of published records
sentinel_payload = json.dumps({"sentinel": True, "total_count": published}).encode('utf-8')  
# Wait for all publish futures to complete before sending the sentinel
for future in publish_futures:
    try:
        future.result(timeout=30)
    except Exception:
        not_published += 1

future = publisher.publish(topic_path, sentinel_payload)
future.result(timeout=10) # Wait for the sentinel message to be published before proceeding   
sentinel_message_sent = time.time()
print(f'\nSentinel published to {TOPIC_ID} PUBLISHING_COMPLETE.')

elapsed_seconds = sentinel_message_sent - start_time
throughput = (published / elapsed_seconds) if elapsed_seconds > 0 else 0.0


print('Summary Statistics:')
print(f'Data accessing at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))}')
print(f'Unique vehicle IDs with data received: {len(vehicle_ids_with_data)}')
print(f'Total messages published: {published}')
print(f'Sentinel message published at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sentinel_message_sent))}')
print(f'Elapsed wall-clock time: {elapsed_seconds:.2f} seconds')
print(f'Throughput (messages/sec): {throughput:.2f}')

