import requests
import json

event = {"transaction": {"source_event_id": "983db327-aa4e-460b-a967-ed8586823e48", "transaction_id": None, "resource_id": "1672fa92d0e644bbafcab19ed30657cb", "retry_after": "2023-05-23 03:34:22.635108"}, "event": {"specversion": "1.0", "id": "983db327-aa4e-460b-a967-ed8586823e48", "source": "https://example.com/event-producer", "type": "com.example.sampletype1", "dataschema": "http://myrepository.com/data-schema", "time": "2023-05-23T03:33:52.297130+00:00", "content-type": "application/cloudevents-bulk+json", "userhash": "e00d019e888653062eb6d5fac8cbbd8d", "token": "7b8d43db43b94746a7dc62b3e161d323"}}

response = requests.post(
    url="http://localhost:9000/2015-03-31/functions/function/invocations",
    headers={},
    data=json.dumps(event)
)

response = 