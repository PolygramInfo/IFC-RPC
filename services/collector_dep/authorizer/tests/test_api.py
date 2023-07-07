import requests
import json
import uuid
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured

attributes = {
        "type": "com.example.sampletype1",
        "content-type": "application/cloudevents-bulk+json",
        "source": "https://example.com/event-producer",
        "dataschema": "http://myrepository.com/data-schema",
        "userhash": "e00d019e888653062eb6d5fac8cbbd8d",
        "token": "7b8d43db43b94746a7dc62b3e161d323"
    }  

data = {
    "transaction_id": uuid.uuid4().hex,
    "somedata": True
}

event = CloudEvent(attributes=attributes, data=data)
headers, body = to_structured(event)

response = requests.post(
    url="https://7jkfh5nb7h.execute-api.us-east-1.amazonaws.com/dev/collector",
    headers=headers,
    data=body
)

print(response.json())