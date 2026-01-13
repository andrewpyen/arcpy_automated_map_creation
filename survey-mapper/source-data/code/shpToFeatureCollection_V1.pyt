import arcpy
import os
import json
import datetime
import tempfile
import shutil

class Toolbox(object):
    def __init__(self):
        self.label = "Recursive Feature Collection Export Toolbox"
        self.alias = "recursive_fc_export"
        self.tools = [RecursiveExportFeatureCollection]

class RecursiveExportFeatureCollection(object):
    def __init__(self):
        self.label = "Recursive Export of Feature Collection JSONs"
        self.description = "Recursively scans folders for shapefiles, reprojects to EPSG:3857 if needed, and exports ArcGIS Online-style Feature Collection JSON files."

    def getParameterInfo(self):
        return [
            arcpy.Parameter(
                displayName="Input Folder (containing shapefiles)",
                name="in_folder",
                datatype="DEFolder",
                parameterType="Required",
                direction="Input"
            ),
            arcpy.Parameter(
                displayName="Output Folder (for JSON files)",
                name="out_folder",
                datatype="DEFolder",
                parameterType="Required",
                direction="Output"
            ),
        ]

    def execute(self, parameters, messages):
        input_folder = parameters[0].valueAsText
        output_folder = parameters[1].valueAsText

        arcpy.env.overwriteOutput = True
        arcpy.AddMessage(f"Scanning: {input_folder}")

        for root, _, files in os.walk(input_folder):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    base_name = os.path.splitext(file)[0]
                    rel_path = os.path.relpath(root, input_folder)
                    output_subfolder = os.path.join(output_folder, rel_path)
                    os.makedirs(output_subfolder, exist_ok=True)
                    json_output = os.path.join(output_subfolder, base_name + ".json")

                    arcpy.AddMessage(f"Exporting: {shp_path}")
                    try:
                        self.process_shapefile(shp_path, json_output)
                        arcpy.AddMessage(f"Saved to: {json_output}")
                    except Exception as e:
                        import traceback
                        arcpy.AddError(f"Error processing {shp_path}: {e}")
                        arcpy.AddError(traceback.format_exc())
                    
        # Create mobile geodatabase
        mobile_gdb_path = os.path.join(output_folder, "output_data.geodatabase")
        if not arcpy.Exists(mobile_gdb_path):
            arcpy.management.CreateMobileGDB(output_folder, "output_data.geodatabase")
            arcpy.AddMessage(f"Created mobile geodatabase at: {mobile_gdb_path}")
        else:
            arcpy.AddMessage(f"Mobile geodatabase already exists at: {mobile_gdb_path}")
        
        # Add shapefiles to the mobile GDB
        for root, _, files in os.walk(input_folder):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    try:
                        arcpy.conversion.FeatureClassToGeodatabase(shp_path, mobile_gdb_path)
                        arcpy.AddMessage(f"Added {file} to mobile geodatabase.")
                    except Exception as e:
                        arcpy.AddWarning(f"Failed to add {file} to GDB: {e}")
        
        #Create disk-based temp folder
        temp_dir = tempfile.mkdtemp()
        arcpy.AddMessage(f"Temporary extraction folder created at: {temp_dir}")

        for root, _, files in os.walk(input_folder):
            for file in files:
                if file.lower().endswith(".lpkx"):
                    source_lpkx = os.path.join(root, file)
                    rel_path = os.path.relpath(root, input_folder)
                    target_dir = os.path.join(output_folder, rel_path)
                    os.makedirs(target_dir, exist_ok=True)
                    target_lpkx = os.path.join(target_dir, file)

                    arcpy.AddMessage(f"Copying LPKX file to: {target_lpkx}")
                    arcpy.management.Copy(source_lpkx, target_lpkx)

                    # Step: Extract and import to mobile geodatabase
                    try:
                        extract_path = os.path.join(temp_dir, os.path.splitext(file)[0])
                        arcpy.AddMessage(f"Extracting {file} to: {extract_path}")
                        arcpy.management.ExtractPackage(source_lpkx, extract_path)

                        # Look for .gdbs and import feature classes
                        for dirpath, _, subfiles in os.walk(extract_path):
                            if dirpath.lower().endswith(".gdb"):
                                arcpy.env.workspace = dirpath
                                fcs = arcpy.ListFeatureClasses()
                                for fc in fcs:
                                    try:
                                        arcpy.AddMessage(f"Importing {fc} to mobile GDB...")
                                        arcpy.conversion.FeatureClassToGeodatabase(fc, mobile_gdb_path)
                                        arcpy.AddMessage(f"Imported {fc} to mobile GDB")
                                    except Exception as import_error:
                                        arcpy.AddWarning(f"Could not import {fc}: {import_error}")
                    except Exception as e:
                        arcpy.AddWarning(f"Failed to unpack or import LPKX '{file}': {e}")

        # Optional cleanup of temp directory
        try:
            shutil.rmtree(temp_dir)
            arcpy.AddMessage("Temporary extraction folder removed.")
        except Exception as e:
            arcpy.AddWarning(f"Could not delete temp folder {temp_dir}: {e}")



    def process_shapefile(self, input_fc, output_path):
        wkid = 102100
        latest_wkid = 3857
        spatial_ref_json = {"wkid": wkid, "latestWkid": latest_wkid}
        original_name = os.path.splitext(os.path.basename(output_path))[0]


        desc = arcpy.Describe(input_fc)
        if desc.spatialReference.factoryCode != latest_wkid:
            projected_fc = os.path.join("in_memory", "temp_proj")
            arcpy.Project_management(input_fc, projected_fc, arcpy.SpatialReference(latest_wkid))
            input_fc = projected_fc
            desc = arcpy.Describe(input_fc)
        else:
            projected_fc = None

        geometry_type = "esriGeometry" + desc.shapeType

        extent = {
            "xmin": desc.extent.XMin,
            "ymin": desc.extent.YMin,
            "xmax": desc.extent.XMax,
            "ymax": desc.extent.YMax,
            "spatialReference": spatial_ref_json
        }

        fields = [f for f in arcpy.ListFields(input_fc) if f.type != "Geometry"]
        field_defs = []
        type_map = {
            "String": "esriFieldTypeString",
            "Integer": "esriFieldTypeInteger",
            "Double": "esriFieldTypeDouble",
            "Single": "esriFieldTypeSingle",
            "SmallInteger": "esriFieldTypeSmallInteger",
            "OID": "esriFieldTypeOID",
            "Date": "esriFieldTypeDate"
        }

        for f in fields:
            field_defs.append({
                "name": f.name,
                "type": type_map.get(f.type, "esriFieldTypeString"),
                "alias": f.aliasName,
                "sqlType": "sqlTypeOther",
                "nullable": f.isNullable,
                "editable": f.editable,
                "domain": None,
                "defaultValue": None
            })

        features = []
        field_names = [f.name for f in fields]
        with arcpy.da.SearchCursor(input_fc, field_names + ["SHAPE@"]) as cursor:
            for row in cursor:
                attr = {}
                for name, val in zip(field_names, row[:-1]):
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        try:
                            attr[name] = int(val.timestamp() * 1000)
                        except (OSError, ValueError):
                            epoch = datetime.datetime(1970, 1, 1)
                            delta = val - epoch
                            attr[name] = int(delta.total_seconds() * 1000)
                    else:
                        attr[name] = val

                shape = row[-1]
                if not shape or (hasattr(shape, "isEmpty") and shape.isEmpty):
                    arcpy.AddWarning("Skipped a feature with null or empty geometry.")
                    continue

                geom = shape.__geo_interface__
                arcgis_geom = {"spatialReference": spatial_ref_json}
                geom_type = geom["type"]

                if geom_type == "Point":
                    arcgis_geom["x"], arcgis_geom["y"] = geom["coordinates"]
                elif geom_type == "LineString":
                    arcgis_geom["paths"] = [geom["coordinates"]]
                elif geom_type == "MultiLineString":
                    arcgis_geom["paths"] = geom["coordinates"]
                elif geom_type == "Polygon":
                    arcgis_geom["rings"] = geom["coordinates"]
                elif geom_type == "MultiPolygon":
                    # Flatten the list of rings
                    arcgis_geom["rings"] = [ring for polygon in geom["coordinates"] for ring in polygon]
                else:
                    arcpy.AddWarning(f"Skipped unsupported geometry type: {geom_type}")
                    continue

                features.append({
                    "attributes": attr,
                    "geometry": arcgis_geom
                })

        layer = {
            "layerDefinition": {
                "currentVersion": 11.2,
                "id": 0,
                "name": original_name,
                "type": "Feature Layer",
                "geometryType": geometry_type,
                "objectIdField": next((f.name for f in fields if f.type == "OID"), "FID"),
                "displayField": "",
                "extent": extent,
                "fields": field_defs,
                "drawingInfo": {
                    "renderer": {
                        "type": "simple",
                        "symbol": {
                            "type": "esriSLS" if "Polyline" in geometry_type else "esriSFS",
                            "style": "esriSLSSolid" if "Polyline" in geometry_type else "esriSFSSolid",
                            "color": [0, 0, 255, 255],
                            "width": 1
                        }
                    }
                }
            },
            "featureSet": {
                "geometryType": geometry_type,
                "features": features,
                "spatialReference": spatial_ref_json
            }
        }

        feature_collection = {"layers": [layer]}

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feature_collection, f, indent=2)

        if projected_fc and arcpy.Exists(projected_fc):
            arcpy.Delete_management(projected_fc)
        
