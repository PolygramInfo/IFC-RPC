from schema_builder import builder, factory, Model
import uuid

myBuilder = builder("myNewSchema")

myBuilder.add_definition("myDefinition", {"type": "string"})\
    .add_property("myFirstProperty", {"$ref": "#/definitions/myDefinition"})\
    .add_properties(
        mySecondProperty={"type": "string", "default": "value", "enum": ["value"]},
        myThirdProperty={"type": "string", "description": "description"}
    ).add_required("myFirstProperty", "mySecondProperty")\
    .allow_additional_properties()

print(myBuilder)

myFactory = myBuilder.factory("myNewSchema")

myInstance = myFactory(
    myFirstProperty="value", 
    mySecondProperty="value",
    myThirdProperty="value",
)

# Here is an example for components.

componentBuilder = builder("Component")

# This builds the component meta-schema.
componentBuilder.add_definition(
    "IDDefinition", {"type": "string", "description": "The ID of the component."}
).add_properties(
    id={"$ref": "#/definitions/IDDefinition", "examples": [uuid.uuid4().hex]},
    describes={"type": "string", "description": "The type of entity described by a schema."},
    subcomponents={"type": "object", "description": "The subcomponents of the component."}
)

component_schema = str(componentBuilder)
print(component_schema)

# This will initialize a new component factory which is used to make instances
# of the component meta-schema.
componentFactory = componentBuilder.factory("Component")

# Here we create a schema for a dimension
dimensionBuilder = builder("Dimension")\
    .add_properties(
        width={"type": "number", "description": "The width of a component."},
        height={"type": "number", "description": "The height of a component."},
        length={"type": "number", "description": "The length of a component."}
    ).add_required("width", "height", "length")

# Now for a wall
myWallSchema = builder("designWall")

myWallSchema.add_definitions(
        IDDefinition=componentBuilder.__dict__["schema"]["definitions"]["IDDefinition"],
        dimensionDef=dimensionBuilder.__dict__["schema"]
    ) \
    .add_properties(
        id={"$ref": "#/definitions/IDDefinition", "examples": [uuid.uuid4().hex]},
        describes={
            "type": "string", 
            "description": "The type of entity described by a schema.",
            "default": "wall",
            "enum": ["wall"]},
        subcomponents={
            "type": "object",
            "properties":{
                "dimensions": {"$ref": "#/definitions/dimensionDef"}
            }
        }
    )

print(myWallSchema.__dict__["schema"])

myWallFactory = myWallSchema.factory("designWall")

print(myWallFactory)

myWall = myWallFactory(
    id=uuid.uuid4().hex,
    describes="wall",
    subcomponents={
        "dimensions":{
        "width": 10,
        "height": 10,
        "length": 10
    }}
)

print(myWall)

import json
with open("myWall.json", "w") as f:
    json.dump(
        myWall.schema,
        f
    )

import boto3
session = boto3.Session(profile_name="default")
dynamodb = session.client("dynamodb")

dynamodb.put_item(
    TableName="SchemaRegistry",
    Item={
        "schema_name": {"S": "test_2"},
        "domain": {"S": "test"},
        "schema": {"S": json.dumps(myWall.schema)}
    }
)