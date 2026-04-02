import os
from wsgiref import validate
from MyMQTT import MyMQTT
import cherrypy
from datetime import datetime
from abc import ABC, abstractmethod
import requests as req
from jsonschema import validate, ValidationError

class HistoryTracking(ABC):
    SENSOR_DATA_SCHEMA = {
        "type": "object",
        "required": ["data"],
        "optional": ["timestamp"],
        "properties": {
            "data": {"type": "object", "additionalProperties": {"type": "number"}},
            "timestamp": {"type": "string", "format": "date-time"}
        }
    }

    exposed = True

    def __init__(self, broker_host, broker_port, catalog, address) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.catalog = catalog
        self.address = address

        if not self.broker_port:
            raise ValueError("Broker Port must be provided")
        self.broker_port = int(self.broker_port)
        
        if not self.catalog:
            raise ValueError("Catalog address must be provided")
        
        if not self.address:
            raise ValueError("Service address must be provided")
        
        try:
            self.register()
        except Exception as e:
            print("Error while registering: ", e)

        self.mqtt_client = MyMQTT("HistoryTrackingClientLocal", self.broker_host, self.broker_port, self)

    def register(self) -> None:
        data = {
            "service_type": "history-tracking",
            "address": self.address,
            "description": "History Tracking adaptor that handles storage and retrieval of historical readings"
        }

        req.post(f"http://{self.catalog}/service", json=data)

    def Start(self):
        self.mqtt_client.start()
        self.mqtt_client.mySubscribe("devices/+/readings/#")
    
    def Stop(self):
        self.mqtt_client.stop()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def GET(self, *uri, **params):

        device_id = params.get("device_id")
        start_time = params.get("start_time")
        end_time = params.get("end_time")

        if not all([device_id, start_time, end_time]):
            raise cherrypy.HTTPError(400, "Missing device_id, start_time or end_time")
        
        try:
            start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
            end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            raise cherrypy.HTTPError(400, "Invalid date format. Use YYYY-MM-DDTHH:MM:SS")

        if start_dt >= end_dt:
            raise cherrypy.HTTPError(400, "start_time must be before end_time")

        try:
            readings = self.retrieve_history(device_id, start_dt, end_dt)
            return {"readings": readings}
        except Exception as e:
            cherrypy.log(f"Database Error: {str(e)}") 
            raise cherrypy.HTTPError(500, "Internal Server Error")


    def notify(self, topic, msg):
        try:
            print(f"Message received on {topic}: {msg}")
            device_id = topic.split("/")[1]
            validate(instance=msg, schema=self.SENSOR_DATA_SCHEMA)

            ts = msg.get("timestamp", datetime.now().isoformat())
            data = msg.get("data")
            self.store_in_history(device_id, ts, data)
            
        except ValidationError as e:
            print("Invalid message", e)
        except Exception as e:
            print("Error decoding message", e)

    @abstractmethod
    def store_in_history(self, device_id, ts, data):
        # Placeholder for storing the message in a history tracking system
        pass

    @abstractmethod
    def retrieve_history(self, device_id, start_time, end_time):
        # Placeholder for retrieving historical data
        pass