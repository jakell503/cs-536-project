import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filename = "assertions.log",
    filemode ='a'
)

PDXAREA_LAT_MIN, PDXAREA_LAT_MAX =  45.0,  46.0
PDXAREA_LON_MIN, PDXAREA_LON_MAX = -123.5, -122.0


def validate_record(record):
    """
    Run all implemented validation assertions against a single breadcrumb record.

    Parameters
    ----------
    record : dict
        A single breadcrumb record as a Python dictionary.

    Returns
    -------
    bool
        True if the record passes all assertions, False if any assertion fails.
    """
    violations = []

    # --- Assertion 1: GPS_LATITUDE must be non-null and in [-90, 90] (Limit) ---
    lat = record.get("GPS_LATITUDE")
    if lat is None or not (PDXAREA_LAT_MIN <= lat <= PDXAREA_LAT_MAX):
        violations.append(f"A1: GPS_LATITUDE is null or out of range {[PDXAREA_LAT_MIN, PDXAREA_LAT_MAX]}")

    # --- Assertion 2: GPS_LONGITUDE must be non-null and in [-180, 180] (Limit) ---
    lon = record.get("GPS_LONGITUDE")
    if lon is None or not (PDXAREA_LON_MIN <= lon <= PDXAREA_LON_MAX):
        violations.append(f"A2: GPS_LONGITUDE is null or out of range {[PDXAREA_LON_MIN, PDXAREA_LON_MAX]}")

    # # --- Assertion 3: ACT_TIME must be non-null and non-negative (Limit) ---
    act_time = record.get("ACT_TIME")
    if act_time is None or act_time < 0:
        violations.append("A3: ACT_TIME is null or out of range (<0)")

    # # --- Assertion 4: EVENT_NO_TRIP must be non-null (Existence) ---
    trip_num = record.get("EVENT_NO_TRIP")
    if trip_num is None:
        violations.append("A4: EVENT_NO_TRIP is null")

    # # --- Assertion 5: EVENT_NO_STOP must be less than or equal to EVENT_NO_TRIP (Logical) ---
    stop_num = record.get("EVENT_NO_STOP")
    if stop_num is None or stop_num < trip_num:
        violations.append("A5: EVENT_NO_STOP is null or invalid")

    # --- Log and return result ---
    if violations:
        for msg in violations:
            logging.warning("VALIDATION VIOLATION [%s] | record: %s", msg, record)
        return violations
   
    return []


print("Validation functions defined.")
