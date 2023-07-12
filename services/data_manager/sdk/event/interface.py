from __future__ import annotations

from cloudevents.http import CloudEvent
from cloudevents.conversion import to_binary, to_structured
from cloudevents.conversion import from_http, from_json
import requests
import json

class Event(CloudEvent):
    # prototype = CloudEvent()

    @staticmethod
    def deserialize(response:requests.Response)->Event:
        event = json.loads(response.json())
        print(type(event))
        headers = {k:v for k,v in event.items() if k != "data"}
        data = event.get("data")

        return(from_http(
            event_type=CloudEvent,
            headers=headers,
            data=json.dumps(data)))
    
    @staticmethod
    def serialize(headers:dict, data:dict)->Event:
        return(
            super().__init__(attributes=headers, data=data)
        )
    
    @staticmethod
    def transmit(event:Event, url:str="http://localhost:5000"):
        headers, data = to_structured(event)
        print(f"Int Headers: {headers}")
        response = requests.post(
            url=url,
            data=data,
            headers=headers
        )

        return(response)