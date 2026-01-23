import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Connection Setup
token = "my-super-secret-token-123"
org = "my-iot-org"
url = "http://localhost:8086"
bucket = "sensor-data"

client = InfluxDBClient(url=url, token=token, org=org)

# 2. Insert Data (The "Write")
write_api = client.write_api(write_options=SYNCHRONOUS)

# Create a data point: Measurement=weather, Tag=location, Field=temp
point = Point("environment") \
    .tag("location", "home-office") \
    .field("temperature", 23.5) \
    .field("humidity", 45.0)

print("Writing data to InfluxDB...")
write_api.write(bucket=bucket, org=org, record=point)

# 3. Retrieve Data (The "Query")
# We use 'Flux', InfluxDB's query language. 
# This looks for everything in the last 10 minutes.
query_api = client.query_api()

query = f"""from(bucket: "{bucket}")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "environment")"""

print("\nRetrieving records:")
tables = query_api.query(query, org=org)

for table in tables:
    for record in table.records:
        print(f"Time: {record.get_time()} | {record.get_field()}: {record.get_value()}")

client.close()