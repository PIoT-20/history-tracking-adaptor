from HistoryTracking import HistoryTracking
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

class InfluxDbHistoryTracking(HistoryTracking):
    def __init__(self) -> None:
        super().__init__()
        self.token = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token-123")
        self.org = os.getenv("INFLUXDB_ORG", "PIoT-20")
        self.url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.bucket = os.getenv("INFLUXDB_BUCKET", "sensor-data")

    def Start(self):
        super().Start()
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)

    def Stop(self):
        self.client.close()
        super().Stop()

    def store_in_history(self, device_id, data):
        print("Storing in DB:", data)
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        points = []
        for measurement, value in data.items():
            point = Point(measurement).tag("DID", device_id).field("value", value)
            points.append(point)
        print(write_api.write(bucket=self.bucket, record=points))


    def retrieve_history(self, device_id, start_time: datetime, end_time: datetime):
        query_api = self.client.query_api()

        # Query matches all measurements for this device
        query = f"""from(bucket: "{self.bucket}")
                |> range(start: start_time_param, stop: end_time_param)
                |> filter(fn: (r) => r.DID == device_id_param)"""
        
        params = {
            "start_time_param": start_time,
            "end_time_param": end_time,
            "device_id_param": device_id
        }

        try:
            tables = query_api.query(query, org=self.client.org, params=params)
        except Exception as e:
            # Log and re-raise so the HTTP layer returns a 500 and the logs
            # contain the original error and query context.
            print(f"InfluxDB query failed: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            raise

        result = {}
        for table in tables:
            for record in table.records:
                time_key = record.get_time().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                measurement = record.get_measurement()
                value = record.get_value()

                if time_key not in result:
                    result[time_key] = {}
                
                result[time_key][measurement] = value

        ret = []
        for key, value in result.items():
            ret.append({
                "timestamp": key, 
                "value": value
            })

        return ret