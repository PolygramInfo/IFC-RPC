import schema_builder
import uuid

from metaSchema import generate_component, generate_entity
from commonComponents import generate_dimensions

# Setup reusable pieces (i.e., meta schema etc.)
component = generate_component()
dimensions = generate_dimensions()

def energy_wall():
    wallSchema = schema_builder.builder("energy.Wall")\
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
        adiabatic={"type":"boolean", "description":"Indicates if a wall is adiabatic.", "default":False}
    )\
    .allow_additional_properties()

    return wallSchema

def energy_roof():
    roofSchema = schema_builder.builder("energy.Roof")\
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

def energy_floor():
    floorSchema = schema_builder.builder("energy.Floor")\
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

def energy_window():
    windowSchema = schema_builder.builder("energy.Window")\
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

    return windowSchema  

def energy_door():
    doorSchema = schema_builder.builder("energy.Door")\
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

    return doorSchema   

def generate_energyPlusMaterialNoMass():
    matNoMass = schema_builder.builder("energy.energyPlusMaterialNoMass")\
    .add_properties(
        name={"type":"string"}
        roughness={"type":"number"},
        thermalResistance={"type":"number"},
        thermalAbsorptance={"type":"number"},
        solarAbsorptance={"type":"number"},
        visibleAbsorptance={"type":"number"}
    )\
    .add_required("name","roughness", "thermalResistance", "thermalAbsorptance", "solarAbsorptance", "visibleAbsorptance")

    return matNoMass 

def energy_material():
    noMasMat = generate_energyPlusMaterialNoMass()

    materialSchema = schema_builder.builder("energy.Material")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        NoMassDef=noMasMat.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"material"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "energyPlusMaterialNoMass":{"$ref":"#/definitions/NoMassDef"}
            }
        }

    )\
    .allow_additional_properties()

    return materialSchema       
   
def generate_energyPlusWindowMaterialSimpleGlazing():
    matSimpleGlazing = schema_builder.builder("energy.energyPlusWindowMaterialSimpleGlazing")\
    .add_properties(
        name={"type":"string"}
        uFactor={"type":"number"},
        solarHeatGianCoefficient={"type":"number"},
        visibleTransmittance={"type":"number"}
  
    )\
    .add_required("name", "uFactor", "solarHeatGianCoefficient", "visibleTransmittance")

    return matNoMass 

def energy_window_material():
    noMasWinMat = generate_energyPlusWindowMaterialSimpleGlazing()

    winMaterialSchema = schema_builder.builder("energy.WindowMaterial")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        NoMasWinMat=noMasMat.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"material"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "energyPlusWindowMaterialSimpleGlazing":{"$ref":"#/definitions/NoMasWinMat"}
            }
        }
    )\
    .allow_additional_properties()

    return winMaterialSchema       

def generate_energyPlusSpaceProgram():
    energyPlusProgram = schema_builder.builder("energy.energyPlusSpaceProgram")\
    .add_properties(
        name={"type":"string"}
        occupancy={"type":"number"},
        monSchedule={"type":"number"},
        tueSchedule={"type":"number"},
        wenSchedule={"type":"number"},
        thuSchedule={"type":"number"}, 
        friSchedule={"type":"number"},
        satSchedule={"type":"number"},
        sunSchedule={"type":"number"},      
    )\
    .add_required("name","occupancy", "monSchedule", "tueSchedule", "wenSchedule", "thuSchedule", "friSchedule", "satSchedule", "sunSchedule")

    return energyPlusProgram     

def generate_energyPlusHVAC():
    energyPlusHVAC = schema_builder.builder("energy.energyPlusHVAC")\
    .add_properties(
        name={"type":"string"}
        MaxHeatingSupplyAirTemp={"type":"number"},
        MaxCoolingSupplyAirTemp={"type":"number"},
        MaxHeatingSupplyAirHumRatio={"type":"number"},
        MaxCoolingSupplyAirHumRatio={"type":"number"},
        HeatingLimit={"type":"number"},
        CoolingLimit={"type":"number"}
    )\
    .add_required("name","MaxHeatingSupplyAirTemp", "MaxCoolingSupplyAirTemp", "MaxHeatingSupplyAirHumRatio", "MaxCoolingSupplyAirHumRatio", "HeatingLimit", "CoolingLimit")

    return energyPlusHVAC    

def energy_space():
    energyPlusProgram = generate_energyPlusSpaceProgram()
    energyPlusHVAC = generate_energyPlusHVAC()
    spaceSchema = schema_builder.builder("energy.Space")\
    .add_definitions(
        IDDefinition=component.__dict__["schema"]["definitions"]["IDDefinition"],
        DescribesDef=component.__dict__["schema"]["properties"]["describes"],
        EnergyPlusProgramDef=energyPlusProgram.__dict__["schema"],
        EnergyPlusHVACDef=energyPlusHVAC.__dict__["schema"]
    )\
    .add_properties(
        id={"$ref":"#/definitions/IDDefinition"},
        describes={"$ref":"#/definitions/DescribesDef","default":"space"},
        subcomponents={
            "title":"Subcomponents",
            "properties":{
                "dimensions":{"$ref":"#/definitions/DimensionDef"},
                "energyPlusProgram":{"$ref":"#/definitions/EnergyPlusProgramDef"},
                "energyPlusHVAC":{"$ref":"#/definitions/EnergyPlusHVACDef"}
            }
        },
        volume={"type":"number", "description":"Indicates the volume of the space."},
        area={"type":"number", "description":"Indicates the area of the space."}
    )\
    .allow_additional_properties()

    return spaceSchema

if __name__ == "__main__":
    import json

    with open("wall_energy_schema.schema.json","w") as f:
        json.dump(energy_wall().__dict__["schema"],f,indent=4)

    with open("window_energy_schema.schema.json","w") as f:
        json.dump(energy_window().__dict__["schema"],f,indent=4)

    with open("roof_energy_schema.schema.json","w") as f:
        json.dump(energy_roof().__dict__["schema"],f,indent=4)

    with open("floor_energy_schema.schema.json","w") as f:
        json.dump(energy_floor().__dict__["schema"],f,indent=4)

    with open("door_energy_schema.schema.json","w") as f:
        json.dump(energy_door().__dict__["schema"],f,indent=4)

    with open("space_energy_schema.schema.json","w") as f:
        json.dump(energy_space().__dict__["schema"],f,indent=4)

    with open("window_material_energy_schema.schema.json","w") as f:
        json.dump(energy_window_material().__dict__["schema"],f,indent=4)

    with open("material_energy_schema.schema.json","w") as f:
        json.dump(energy_material().__dict__["schema"],f,indent=4)
