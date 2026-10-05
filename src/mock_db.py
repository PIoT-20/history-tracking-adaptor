import datetime
import os
import random

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


# ---- Parameters ----
DEVICE_ID = "mock-device-01"
SENSOR_TYPE = "TWA_NOISE"           # TWA_NOISE | WBGT | NVIR_VENTILATION
QUALITY_LEVEL = "HIGH"            # HIGH | NORMAL | LOW
CHAOS_LEVEL = "LOW"              # LOW | NORMAL | HIGH
START_TIME = "2026-01-03T00:00:00"  # ISO-8601, naive is treated as UTC
END_TIME = "2026-01-04T00:00:00"
FREQUENCY_SECONDS = 60
CHUNK_SIZE = 50_000                 # rows per write call

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Same factors as device-gateway/src/Emulators.py
QUALITY_MEAN_FACTOR = {"HIGH": 0.25, "NORMAL": 0.50, "LOW": 0.75}
CHAOS_STD_FACTOR = {"LOW": 0.05, "NORMAL": 0.12, "HIGH": 0.25}

# measurement -> (low, high), same ranges as the device-gateway emulators
SENSORS = {
    "TWA_NOISE": {"db": (70, 100)},
    "WBGT": {"Tnw": (18, 30), "Tg": (18, 34)},
    "NVIR_VENTILATION": {"T": (15, 30), "P": (800, 1200), "CO2": (300, 1200)},
}


# OS-entropy generator: no shared seed state, so each device gets independent readings
rng = random.SystemRandom()


def gauss(low, high):
    """Gaussian sample clamped to [low, high], shaped by quality/chaos level."""
    span = high - low
    mean = low + span * QUALITY_MEAN_FACTOR[QUALITY_LEVEL]
    std = span * CHAOS_STD_FACTOR[CHAOS_LEVEL]
    return max(low, min(high, round(rng.gauss(mean, std))))


def parse_time(value):
    ts = datetime.datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    return ts


def validate(start, end):
    if not DEVICE_ID:
        raise ValueError("DEVICE_ID must be set")
    if SENSOR_TYPE not in SENSORS:
        raise ValueError(f"SENSOR_TYPE must be one of {list(SENSORS)}")
    if QUALITY_LEVEL not in QUALITY_MEAN_FACTOR:
        raise ValueError(f"QUALITY_LEVEL must be one of {list(QUALITY_MEAN_FACTOR)}")
    if CHAOS_LEVEL not in CHAOS_STD_FACTOR:
        raise ValueError(f"CHAOS_LEVEL must be one of {list(CHAOS_STD_FACTOR)}")
    if FREQUENCY_SECONDS <= 0:
        raise ValueError("FREQUENCY_SECONDS must be positive")
    if end <= start:
        raise ValueError("END_TIME must be after START_TIME")
    if not (INFLUXDB_TOKEN and INFLUXDB_ORG and INFLUXDB_BUCKET):
        raise ValueError("INFLUXDB_TOKEN, INFLUXDB_ORG and INFLUXDB_BUCKET must be set")


def generate_readings(start, end):
    """Build the timestamps and one value list per measurement, like _run_dump_mode."""
    samples = int((end - start).total_seconds() / FREQUENCY_SECONDS)
    if samples < 1:
        raise ValueError("Time range is shorter than FREQUENCY_SECONDS")

    fields = SENSORS[SENSOR_TYPE]
    timestamps = [start + datetime.timedelta(seconds=i * FREQUENCY_SECONDS) for i in range(samples)]
    columns = {m: [gauss(low, high) for _ in range(samples)] for m, (low, high) in fields.items()}
    return timestamps, columns


def main():
    start = parse_time(START_TIME)
    end = parse_time(END_TIME)
    validate(start, end)

    timestamps, columns = generate_readings(start, end)

    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for measurement, values in columns.items():
            # Same layout as InfluxDbHistoryTracking.store_in_history: measurement, tag DID, field value
            frame = pd.DataFrame({"time": timestamps, "DID": DEVICE_ID, "value": values})
            for offset in range(0, len(frame), CHUNK_SIZE):
                write_api.write(
                    bucket=INFLUXDB_BUCKET,
                    record=frame.iloc[offset:offset + CHUNK_SIZE],
                    data_frame_measurement_name=measurement,
                    data_frame_tag_columns=["DID"],
                    data_frame_timestamp_column="time",
                )
    finally:
        client.close()

    print(f"Inserted {len(timestamps)} readings x {len(columns)} measurements "
          f"for {DEVICE_ID} from {start.isoformat()} to {end.isoformat()}")


if __name__ == "__main__":
    main()
