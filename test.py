import sdk
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured, to_binary, to_json
import requests
import uuid

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


"""attributes = {
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

print(f"JSON Version: {type(to_json(event))}")

print(to_json(event))

with open("byte_encoded.txt","wb") as file:
    for i in to_json(event):
        file.write(bytes([i]))"""

#response = sdk.Event.transmit(event=event)

#print(response.json())

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

AWS_API_EVENT = {
  "body": "{}",
  "resource": "/{proxy+}",
  "path": "/path/to/resource",
  "httpMethod": "POST",
  "isBase64Encoded": True,
  "queryStringParameters": {
    "foo": "bar"
  },
  "multiValueQueryStringParameters": {
    "foo": [
      "bar"
    ]
  },
  "pathParameters": {
    "proxy": "/path/to/resource"
  },
  "stageVariables": {
    "baz": "qux"
  },
  "headers": {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Cache-Control": "max-age=0",
    "CloudFront-Forwarded-Proto": "https",
    "CloudFront-Is-Desktop-Viewer": "true",
    "CloudFront-Is-Mobile-Viewer": "false",
    "CloudFront-Is-SmartTV-Viewer": "false",
    "CloudFront-Is-Tablet-Viewer": "false",
    "CloudFront-Viewer-Country": "US",
    "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Custom User Agent String",
    "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
    "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
    "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
    "X-Forwarded-Port": "443",
    "X-Forwarded-Proto": "https"
  },
  "multiValueHeaders": {
    "Accept": [
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    ],
    "Accept-Encoding": [
      "gzip, deflate, sdch"
    ],
    "Accept-Language": [
      "en-US,en;q=0.8"
    ],
    "Cache-Control": [
      "max-age=0"
    ],
    "CloudFront-Forwarded-Proto": [
      "https"
    ],
    "CloudFront-Is-Desktop-Viewer": [
      "true"
    ],
    "CloudFront-Is-Mobile-Viewer": [
      "false"
    ],
    "CloudFront-Is-SmartTV-Viewer": [
      "false"
    ],
    "CloudFront-Is-Tablet-Viewer": [
      "false"
    ],
    "CloudFront-Viewer-Country": [
      "US"
    ],
    "Host": [
      "0123456789.execute-api.us-east-1.amazonaws.com"
    ],
    "Upgrade-Insecure-Requests": [
      "1"
    ],
    "User-Agent": [
      "Custom User Agent String"
    ],
    "Via": [
      "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)"
    ],
    "X-Amz-Cf-Id": [
      "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA=="
    ],
    "X-Forwarded-For": [
      "127.0.0.1, 127.0.0.2"
    ],
    "X-Forwarded-Port": [
      "443"
    ],
    "X-Forwarded-Proto": [
      "https"
    ]
  },
  "requestContext": {
    "accountId": "123456789012",
    "resourceId": "123456",
    "stage": "prod",
    "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
    "requestTime": "09/Apr/2015:12:34:56 +0000",
    "requestTimeEpoch": 1428582896000,
    "identity": {
      "cognitoIdentityPoolId": None,
      "accountId": None,
      "cognitoIdentityId": None,
      "caller": None,
      "accessKey": None,
      "sourceIp": "127.0.0.1",
      "cognitoAuthenticationType": None,
      "cognitoAuthenticationProvider": None,
      "userArn": None,
      "userAgent": "Custom User Agent String",
      "user": None
    },
    "path": "/prod/path/to/resource",
    "resourcePath": "/{proxy+}",
    "httpMethod": "POST",
    "apiId": "1234567890",
    "protocol": "HTTP/1.1"
  }
}

import json
event = CloudEvent(attributes=attributes, data=data)
headers, data = to_structured(event)
#AWS_API_EVENT["body"] = data.decode("utf-8")

print(AWS_API_EVENT)

response = requests.post(
    url="http://localhost:9000/2015-03-31/functions/function/invocations",
    headers=headers,
    data=data
)

print(f"Response: {response.json()}")