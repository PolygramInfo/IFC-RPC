import sdk
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured, to_binary, to_json
import requests

schema = {
    "title": "Test Schema",
    "properties": {
        "name": {"type": "string"},
        "abbr": {"type": "string"},
    },
    "required": ["name"]
}

data = {"name": "name", "abbr": "my name"}

constructor = sdk.schema_factory(schema)

instance = constructor(**data)

cd = {"name":"new_name"}

instance.update(cd)

cd = {"name": "other name", "abbr": "nnnnn"}

instance.update(cd)

print(instance.clone())


attributes = {
    "type": "com.example.sampletype1",
    "content-type": "application/cloudevents-bulk+json",
    "source": "https://example.com/event-producer",
    "dataschema": "http://myrepository.com/data-schema"
}

data = {"message": "value"}

event = sdk.Event(
    attributes=attributes,
    data=data
)

response = sdk.Event.transmit(event=event)

print(response.json())

print(sdk.Event.deserialize(response=response))
