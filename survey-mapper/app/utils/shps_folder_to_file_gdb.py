import arcpy
import os
import datetime
import tempfile
import shutil

# --- USER INPUT ---
input_folder = "C:\\Users\\geoinformatica\\Documents\\repos\\GIS-MAPCREATION\\survey-mapper\\source-data\\SWGO_x93y1634_DOT_shps"
output_gdb_name = "DOT_Shps_Combined.gdb"
output_folder = "C:\\Users\\geoinformatica\\Documents\\repos\\GIS-MAPCREATION\\survey-mapper\\source-data"

# --- Derived Paths ---
output_gdb_path = os.path.join(output_folder, output_gdb_name)

# --- Create Output GDB ---
if not arcpy.Exists(output_gdb_path):
    arcpy.CreateFileGDB_management(output_folder, output_gdb_name)
    print(f"[✔] Created output GDB: {output_gdb_path}")
else:
    print(f"[!] Output GDB already exists: {output_gdb_path}")

# --- Import Shapefiles ---
for file in os.listdir(input_folder):
    if file.lower().endswith(".shp"):
        shp_path = os.path.join(input_folder, file)
        try:
            print(f"[📄] Importing shapefile: {file}")
            arcpy.FeatureClassToGeodatabase_conversion(shp_path, output_gdb_path)
        except Exception as e:
            print(f"[ERROR] Failed to import shapefile {file}: {e}")

# --- Import .lpkx contents (including annotation feature classes) ---
for file in os.listdir(input_folder):
    if file.lower().endswith(".lpkx"):
        lpkx_path = os.path.join(input_folder, file)
        print(f"[📦] Processing LPKX: {file}")
        try:
            with tempfile.TemporaryDirectory() as tmp_extract_dir:
                arcpy.ExtractPackage_management(lpkx_path, tmp_extract_dir)

                # Look for a GDB in the extracted contents
                for root, dirs, _ in os.walk(tmp_extract_dir):
                    for dir in dirs:
                        if dir.lower().endswith(".gdb"):
                            extracted_gdb = os.path.join(root, dir)
                            arcpy.env.workspace = extracted_gdb
                            feature_classes = arcpy.ListFeatureClasses()
                            if feature_classes:
                                print(f"[📁] Found GDB: {dir}, importing {len(feature_classes)} feature classes...")
                                for fc in feature_classes:
                                    full_fc_path = os.path.join(extracted_gdb, fc)
                                    try:
                                        arcpy.FeatureClassToGeodatabase_conversion(full_fc_path, output_gdb_path)
                                        print(f"  └── Imported: {fc}")
                                    except Exception as fe:
                                        print(f"[ERROR] Failed to import {fc}: {fe}")
        except Exception as e:
            print(f"[ERROR] Failed to process {file}: {e}")

print(f"[✅] All shapefiles and LPKX contents (including annotation) have been imported to {output_gdb_path}")
