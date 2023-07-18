import boto3

session = boto3.Session()
dynamodb = session.client('dynamodb')

response = dynamodb.query(
    TableName="Resources",
    KeyConditionExpression="resource_id = :resource_id",
    FilterExpression="published = :attrValue",
    ExpressionAttributeValues={
        ":resource_id": {"S": "c34d6b571696419dbb052e553661211a"},
        ":attrValue": {"BOOL": True}
    })["Items"]

print(response)