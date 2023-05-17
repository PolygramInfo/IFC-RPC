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
        return(from_http(
            event_type=CloudEvent,
            headers=response.headers,
            data=response.json()))
    
    @staticmethod
    def serialize(headers:dict, data:dict)->Event:
        return(
            super().__init__(attributes=headers, data=data)
        )
    
    @staticmethod
    def transmit(event:Event):
        headers, data = to_binary(event)
        response = requests.post(
            url="http://localhost:5000/",
            data=data,
            headers=headers
        )

        return(response)