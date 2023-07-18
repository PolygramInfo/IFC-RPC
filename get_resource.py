import boto3
import sdk
import json
import uuid

resource_id = "00a3ea0afad94f0d877d3dd43cff7d3c"

s3 = boto3.client("s3")
bucket = "pg-resource-registery"
key = f"resources/{resource_id}.json"

resource = s3.get_object(
    Bucket=bucket,
    Key=key
)

resource_body = json.loads(json.loads(resource["Body"].read().decode("utf-8"))["schema"]["S"])

# Only use this part.
schema = sdk.schema_factory(resource_body)
instance = schema(
    id = uuid.uuid4().hex,
    describes = "wall",
    subcomponents = {
        "properties":{
            "dimensions": {
                "length":10,
                "height":10,
                "width":0.5
            }
        }
    }
)

with open("designWall.json","w") as f:
    in_schema = instance.schema
    print(json.dumps(in_schema, indent=4))
    json.dump(instance.schema, f,indent=4)

with open("designWall.instance.json", "w") as f:
    json.dump(instance, f, indent=4)