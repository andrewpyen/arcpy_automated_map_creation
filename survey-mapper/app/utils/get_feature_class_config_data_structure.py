import arcpy
import os
import json

def extract_feature_class_metadata(gdb_path):
    """
    Extracts full paths and field metadata for all feature classes in a File Geodatabase.

    Args:
        gdb_path (str): Path to the .gdb folder.

    Returns:
        list[dict]: Feature class metadata with full paths and field definitions.
    """
    if not arcpy.Exists(gdb_path):
        raise FileNotFoundError(f"GDB not found: {gdb_path}")
    
    gdb_full_path = os.path.abspath(gdb_path)
    arcpy.env.workspace = gdb_full_path

    feature_classes = []

    # Top-level FCs
    for fc in arcpy.ListFeatureClasses():
        feature_classes.append(os.path.join(gdb_full_path, fc))

    # FCs inside datasets
    datasets = arcpy.ListDatasets('', 'Feature')
    if datasets:
        for ds in datasets:
            ds_path = os.path.join(gdb_full_path, ds)
            for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
                feature_classes.append(os.path.join(ds_path, fc))

    results = []

    for fc_path in feature_classes:
        fc_name = os.path.basename(fc_path)
        field_list = []

        for field in arcpy.ListFields(fc_path):
            field_list.append({
                "name": field.name,
                "alias": field.aliasName,
                "type": field.type,
                "length": field.length if field.type == "String" else ""
            })

        results.append({
            "feature_class_name_source": fc_name,
            "shapefile_name_target": fc_name
        })

    return results


# --- USER INPUT ---
gdb_path = "C:\\Users\\geoinformatica\\Documents\\repos\\GIS-MAPCREATION\\survey-mapper\\source-data\\gdbs\\SAZ_Heath_20250611.gdb"
output_json = "C:\\Users\\geoinformatica\\Documents\\repos\\GIS-MAPCREATION\\survey-mapper\\config\\config_feature_class_fields.json"

# Run the extraction
fc_metadata = extract_feature_class_metadata(gdb_path)

# Save to JSON
with open(output_json, "w", encoding="utf-8") as f:
    json.dump(fc_metadata, f, indent=2)

print(f"Metadata saved to: {output_json}")
