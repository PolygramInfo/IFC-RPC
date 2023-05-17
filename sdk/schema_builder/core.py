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