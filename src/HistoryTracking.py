import os
from MyMQTT import MyMQTT
import cherrypy
from datetime import datetime
import json

class HistoryTracking:
    exposed = True

    def __init__(self) -> None:
        broker_host = os.getenv("BROKER_HOST", "127.0.0.1")
        try:
            broker_port = int(os.getenv("BROKER_PORT", "1883"))
        except ValueError:
            broker_port = 1883

        self.mqtt_client = MyMQTT("HistoryTrackingClientLocal", broker_host, broker_port, self)

    def Start(self):
        self.mqtt_client.start()
        self.mqtt_client.mySubscribe("devices/+/readings/#")
    
    def Stop(self):
        self.mqtt_client.stop()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def GET(self, device_id=None, start_time=None, end_time=None):
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


    def store_in_history(self, device_id, data):
        # Placeholder for storing the message in a history tracking system
        pass

    
    def retrieve_history(self, device_id, start_time, end_time):
        # Placeholder for retrieving historical data
        pass