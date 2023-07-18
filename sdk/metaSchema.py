from .schema_builder import builder
import uuid
import hashlib

def generate_component():
    componentSchema = builder("Component")\
    .add_definitions(
        IDDefinition={"type":"string", "description":"Component identifier, should be a 36 character UUID or hex hashed UUID."})\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"type":"string", "description":"Type of entities described by this component schema", "enum":["wall","floor"]},
        subcomponents={"type":"object", "description":"Constiuent schemata"})\
    .add_required("id","subcomponents")\
    .allow_additional_properties()

    return componentSchema

def generate_entity():
    entitySchema = builder("Entity")\
    .add_definitions(
        IDDefinition={"type":"string", "description":"Identifier for the entity"},
        TypeDef={"type":"string", "description":"Limits the types of components that can be registered to this entity."})\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        primitive_type={"$ref":"#/definitions/TypeDef"})\
    .add_required("id")

    return entitySchema

if __name__ == "__main__":

    entityFactory = generate_entity().factory("myEntity")
    entity = entityFactory(
        id=uuid.uuid4().hex,
        primitive_type="wall"
    )    

    print(entity)