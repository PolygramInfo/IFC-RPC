from .schema_builder import builder
import uuid

from .metaSchema import generate_component, generate_entity
from .commonComponents import generate_dimensions

# Setup reusable pieces (i.e., meta schema etc.)
component = generate_component()
dimensions = generate_dimensions()

def design_wall():
    wallSchema = builder("design.Wall")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        DimensionDef=dimensions.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"wall"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"}
            }
        },
        exterior={"type":"boolean", "description":"Indicates if a wall is exterior facing.", "default":True}
    )\
    .allow_additional_properties()

    return wallSchema

def design_roof():
    roofSchema = builder("design.Roof")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        DimensionDef=dimensions.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"wall"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"}
            }
        }
    )\
    .allow_additional_properties()

    return roofSchema    

def design_floor():
    floorSchema = builder("design.Floor")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        DimensionDef=dimensions.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"wall"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"}
            }
        }
    )\
    .allow_additional_properties()

    return floorSchema    

def design_window():
    windowSchema = builder("design.Window")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        DimensionDef=dimensions.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"wall"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"}
            }
        },
        sillHeight={"type":"number", "description":"Indicates the sill height of the window."}
    )\
    .allow_additional_properties()

    return windowSchema  

def design_door():
    doorSchema = builder("design.Door")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        DimensionDef=dimensions.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"wall"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"}
            }
        },
        exterior={"type":"boolean", "description":"Indicates if a wall is exterior facing.", "default":True}
    )\
    .allow_additional_properties()

    return doorSchema  

def design_material():
    materialSchema = builder("design.Material")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"material"},
        category={"type":"string", "description":"Indicates the broad material category."}
    )\
    .allow_additional_properties()

    return materialSchema

def design_space():
    spaceSchema = builder("design.Space")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"space"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/SubcompDef"}
            }
        },
        volume={"type":"number", "description":"Indicates the volume of the space."},
        area={"type":"number", "description":"Indicates the area of the space."},
        name={"type":"string", "description":"Indicates the program type of the space."}
    )\
    .allow_additional_properties()

    return spaceSchema

if __name__ == "__main__":
    import json

    with open("wall_design_schema.schema.json","w") as f:
        json.dump(design_wall().__dict__["schema"],f,indent=4)

    with open("window_design_schema.schema.json","w") as f:
        json.dump(design_window().__dict__["schema"],f,indent=4)

    with open("door_design_schema.schema.json","w") as f:
        json.dump(design_door().__dict__["schema"],f,indent=4)

    with open("roof_design_schema.schema.json","w") as f:
        json.dump(design_roof().__dict__["schema"],f,indent=4)

    with open("floor_design_schema.schema.json","w") as f:
        json.dump(design_floor().__dict__["schema"],f,indent=4)        

    with open("space_design_schema.schema.json","w") as f:
        json.dump(design_space().__dict__["schema"],f,indent=4)

    with open("material_design_schema.schema.json","w") as f:
        json.dump(design_material().__dict__["schema"],f,indent=4)
