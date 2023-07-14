import schema_builder
import metaSchema
import uuid

def generate_dimensions():
    component = metaSchema.generate_component()
    dimensionSchema = schema_builder.builder("Dimensions")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DimensionDef={"type":"number","description":"Value for a linear or metric dimension."}
    )\
    .add_properties(
        length={"$ref":"#/definitions/DimensionDef"},
        width={"$ref":"#/definitions/DimensionDef"},
        height={"$ref":"#/definitions/DimensionDef"}
    )

    return dimensionSchema