from more_itertools import bucket

from InfluxDbHistoryTracking import InfluxDbHistoryTracking
import cherrypy
import os
import json

def json_error_page(status, message, traceback, version):
    # This replaces the default HTML error template
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps({
        "status": status,
        "error": message
    }).encode('utf-8')

if __name__ == "__main__":
    
    broker_host = os.getenv("BROKER_HOST")
    broker_port = int(os.getenv("BROKER_PORT")) 
    catalog = os.getenv("CATALOG_ADDRESS")
    address = os.getenv("HISTORY_TRACKING_ADDRESS")
    url = os.getenv("INFLUXDB_URL")
    token = os.getenv("INFLUXDB_TOKEN")
    org = os.getenv("INFLUXDB_ORG")
    bucket = os.getenv("INFLUXDB_BUCKET")
    
    service = InfluxDbHistoryTracking(
        broker_host=broker_host,
        broker_port=broker_port,
        catalog=catalog,
        address=address,
        token=token,
        org=org,
        url=url,
        bucket=bucket
    )
    
    
    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tools.staticdir.root': os.path.abspath(os.getcwd()),
            'tools.sessions.on': True,
            'tools.json_out.on': True,
            'tools.json_in.on': True,
            'request.show_tracebacks': False,
            'request.show_mismatched_params': False,
            'engine.autoreload.on': True,
            'error_page.default': json_error_page
        },
    }
    service.Start()
    
    cherrypy.config.update({'server.socket_host': '0.0.0.0', "server.socket_port": 8080})
    cherrypy.tree.mount(service, '/readings', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()
    service.Stop()