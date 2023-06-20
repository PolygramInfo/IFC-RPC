from fastapi import FastAPI, Request
from cloudevents.http import CloudEvent
from cloudevents.http import from_http
from cloudevents.conversion import to_json, to_binary
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
    event = from_http(
        headers=request.headers,
        data= await request.body()
    )

    print(event.get_attributes())
    print(event.get_data())

    constructor = schema_builder.factory(known_schema)

    instance = constructor(**event.get_data())

    print(instance.__dict__)

    instance.message = "value2"

    attributes = {
    "type": "com.example.Response",
    "content-type": "application/cloudevents+json",
    "source": "https://example.com/event-producer",
    "dataschema": "http://myrepository.com/data-schema"
    }
    
    response_event = CloudEvent(
        attributes=attributes,
        data=event.get_data()
    )

    headers, response = to_binary()

    return(

    )

if __name__ == "__main__":
    app.run(port=5000)