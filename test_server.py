from fastapi import FastAPI, Request
from cloudevents.http import CloudEvent
from cloudevents.http import from_http
from cloudevents.conversion import to_json
import json

from sdk import schema_builder

known_schema = {
    "title": "myEvent",
    "properties": {
        "message": {"type":"string"}
    }
}

app = FastAPI(
    title=__name__
)

@app.post("/")
async def home(request:Request):
    headers = request.headers
    data = await request.body()
    data = json.loads(data.decode())
    print(headers)
    print(data)

    constructor = schema_builder.factory(known_schema)

    instance = constructor(**data)

    print(instance.__dict__)

    instance.message = "value2"

    attributes = {
    "type": "com.example.Response",
    "content-type": "application/cloudevents+json",
    "source": "https://example.com/event-producer",
    "dataschema": "http://myrepository.com/data-schema"
    }
    
    return(to_json(CloudEvent(attributes=attributes, data=data)))

if __name__ == "__main__":
    app.run(port=5000)