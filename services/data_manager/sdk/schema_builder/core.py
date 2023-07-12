import copy
from hashlib import md5

from jsonschema.validators import validator_for

from .model import Model

def factory(schema, base_class=Model, name=None, resolver=None):
    
    schema = copy.deepcopy(schema)
    resolver = resolver

    class SchemaModel(base_class):
            
        def __init__(self, *args, **kwargs):

            self.__dict__["schema"] = schema
            self.__dict__["resolver"] = resolver

            cls = validator_for(self.schema)
            self.__dict__["validator_instance"] = cls(schema, resolver=resolver) if resolver else cls(schema)


            super().__init__(self, *args, **kwargs)

    if resolver:
        SchemaModel.resolver = resolver
    if name:
        SchemaModel.__name__ = name if name else str(schema.get(
            "name",
            md5(str(schema).encode('utf-8')).hexdigest()))
    
    return SchemaModel

def builder(
        title:str, 
        base_class=Model,
        schema:str="http://json-schema.org/draft-07/schema#", 
        name=None):

    class SchemaConstructor:
        def __init__(self, *args, **kwargs):       
            schema = {
                "title": title,
                "schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }

            self.__dict__["schema"] = schema

        def __call__(self, name):
            return factory(self.__dict__["schema"], name=name)
        
        def __str__(self):
            import json
            return json.dumps(self.__dict__["schema"], indent=4)

        def factory(self, name):
            """
            The factory method is a shortcut for creating a new model from the schema.

            Example:
            mySchema = schema_builder.builder("mySchema")
                .add_property("prop1", {"type": "string"})
                .factory("mySchema")

            myInstance = mySchema("prop1"="value")
            """
            return self.__call__(name)

        def add_property(self, name, schema):
            """
            Add a property to the schema.

            Example:
            schema_builder.builder("mySchema").add_property("prop1", {"type": "string"})
            schema_builder.builder("mySchema").add_property("prop2", {"$ref": "#/definitions/myDefinition"}})

            Note: This method is chainable. This method will raise an exception if the property already exists or if the reference is missing from the definitions.
            """

            if name in self.__dict__["schema"]["properties"]:
                raise ValueError("Property already exists.")

            if "$ref" in schema and schema["$ref"].split("/")[-1] not in self.__dict__.get("schema", {}).get("definitions", {}):
                raise ValueError("Reference does not exist in definitions.")

            self.__dict__["schema"]["properties"][name] = schema
            return self

        def add_properties(self, **kwargs):
            """
            Add properties to the schema.
            
            Example:
            schema_builder.builder("mySchema").add_properties(
                prop1={"type": "string"}, 
                prop2={"$ref": "#/definitions/myDefinition"}}
            )
            """
            for name, schema in kwargs.items():
                self.add_property(name, schema)
            return self
        
        def add_definition(self, name, schema):
            """
            Add a definition to the schema.

            Example:
            schema_builder.builder("mySchema").add_definition("myDefinition", {"type": "string"})
            """
            if not "definitions" in self.__dict__["schema"]:
                self.__dict__["schema"]["definitions"] = {}

            self.__dict__["schema"]["definitions"][name] = schema
            return self
        
        def add_definitions(self, **kwargs):
            """
            Add definitions to the schema.
            
            Example:
            schema_builder.builder("mySchema").add_definitions(
                myDefinition={"type": "string", "default": "value", "enum": ["value"]},
                myDefinition2={"type": "string", "description": "description"}
            )
            """
            for name, schema in kwargs.items():
                self.add_definition(name, schema)
            return self

        def add_required(self, *args):
            """
            Add required properties to the schema.

            Example:
            schema_builder.builder("mySchema").add_required("prop1", "prop2")
            """
            self.__dict__["schema"]["required"] = args
            return self
        
        def allow_additional_properties(self):
            """
            Allow additional properties in the schema. By default this is
            disabled.

            Example:
            schema_builder.builder("mySchema").allow_additional_properties()
            """
            self.__dict__["schema"]["additionalProperties"] = True
            return self
        
    return SchemaConstructor()