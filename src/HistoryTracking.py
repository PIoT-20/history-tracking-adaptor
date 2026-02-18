import os
from MyMQTT import MyMQTT
import cherrypy
from datetime import datetime
from abc import ABC, abstractmethod
import requests as req

class HistoryTracking(ABC):
    exposed = True

    def __init__(self) -> None:
        self.broker_host = os.getenv("BROKER_HOST")
        if not self.broker_host:
            raise ValueError("BROKER_HOST environment variable not set")
        
        self.broker_port = os.getenv("BROKER_PORT")
        if not self.broker_port:
            raise ValueError("BROKER_PORT environment variable not set")
        self.broker_port = int(self.broker_port)
        
        self.catalog = os.getenv("CATALOG_ADDRESS")
        if not self.catalog:
            raise ValueError("CATALOG_ADDRESS environment variable not set")
        
        self.address = os.getenv("HISTORY_TRACKING_ADDRESS")
        if not self.address:
            raise ValueError("HISTORY_TRACKING_ADDRESS environment variable not set")
        
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
            self.store_in_history(device_id, msg)
        except Exception as e:
            print("Error decoding message", e)

    @abstractmethod
    def store_in_history(self, device_id, data):
        # Placeholder for storing the message in a history tracking system
        pass

    @abstractmethod
    def retrieve_history(self, device_id, start_time, end_time):
        # Placeholder for retrieving historical data
        pass