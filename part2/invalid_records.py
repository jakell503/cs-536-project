import json
from datetime import date

# Example: writing invalid records to a file.
# In analysis.py, accumulate invalid records in a list and write them
# to this file when the sentinel is received.

def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid breadcrumb records to a dated JSON file.

    Parameters
    ----------
    invalid_records : list of dict
        Each dict should have a 'record' key (the original data)
        and a 'violations' key (list of assertion violation messages).
    run_date : str, optional
        Date string in YYYY-MM-DD format. Defaults to today.
    """
    if run_date is None:
        run_date = date.today().isoformat()

    filename = f"invalid_data_{run_date}.json"

    with open(filename, "w") as f:
        json.dump(invalid_records, f, indent=2, default=str)

    print(f"Wrote {len(invalid_records)} invalid records to {filename}")


# Example of what an invalid record entry looks like:
example_entry = {
    "violations": ["A1: GPS_LATITUDE is null or out of range [-90, 90]"],
    "record": {
        "EVENT_NO_TRIP": 12345678,
        "OPD_DATE": "15APR2025:00:00:00",
        "VEHICLE_ID": 4100,
        "GPS_LATITUDE": None,
        "GPS_LONGITUDE": -122.65
    }
}
#print(json.dumps(example_entry, indent=2))
