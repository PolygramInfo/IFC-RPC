from cloudevents.http import CloudEvent
from cloudevents.conversion import to_dict

import json

class Event:

    def set_attributes(
            self, 
            service, 
            request_type, 
            userhash, 
            usertoken, 
            transasction_id=None,
            **kwargs):

        self.attributes = {
            "type": f"com.pg.{service}/{request_type}",
            "source": f"localhost:5000/sdk",
            "content-type": "application/cloudevents+json",
            "userhash": userhash,
            "usertoken": usertoken,
            "transactionid": transasction_id,
            "datacontenttype": "application/json",
        }

        self.attributes.update(kwargs)
    
        return self

    def set_schema(self, domain, schema_name):
        self.attributes["dataschema"] = f"{domain}.{schema_name}"

        return self
    
    def set_data(self, **kwargs):

        self.data = kwargs if kwargs else {}

        return self

    def load_data(self, schema, path):

        with open(path, "r") as file:
            data = json.load(file)

        self.set_data(schema, data)

        return self

    def serialize(self)->CloudEvent:
        self.event = CloudEvent(
            self.attributes,
            self.data
        )

        return self.event
    
    @staticmethod
    def deserialize(self, event:CloudEvent)->dict:
        return to_dict(event)

    def to_json(self):
        return json.dumps(to_dict(self.event))        