import os
import arcpy
import zipfile
import tempfile

def convert_zip_to_gdb(zip_path: str, output_dir: str, output_gdb_name: str) -> str:
    """
    Extracts a zipped folder of shapefiles and .lpkx files, and imports all content into a new file geodatabase.

    Args:
        zip_path (str): Path to the uploaded zip file
        output_dir (str): Folder to store the generated .gdb
        output_gdb_name (str): Name of the output geodatabase

    Returns:
        str: Full path to the new file geodatabase
    """
    extract_dir = os.path.join(output_dir, "unzipped")
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    gdb_path = os.path.join(output_dir, output_gdb_name)
    if not arcpy.Exists(gdb_path):
        arcpy.CreateFileGDB_management(output_dir, output_gdb_name)

    for file in os.listdir(extract_dir):
        if file.lower().endswith(".shp"):
            shp_path = os.path.join(extract_dir, file)
            arcpy.FeatureClassToGeodatabase_conversion(shp_path, gdb_path)

    for file in os.listdir(extract_dir):
        if file.lower().endswith(".lpkx"):
            lpkx_path = os.path.join(extract_dir, file)
            with tempfile.TemporaryDirectory() as tmp_lpkx_dir:
                arcpy.ExtractPackage_management(lpkx_path, tmp_lpkx_dir)

                for root, dirs, _ in os.walk(tmp_lpkx_dir):
                    for dir in dirs:
                        if dir.lower().endswith(".gdb"):
                            embedded_gdb = os.path.join(root, dir)
                            arcpy.env.workspace = embedded_gdb
                            for fc in arcpy.ListFeatureClasses():
                                fc_path = os.path.join(embedded_gdb, fc)
                                arcpy.FeatureClassToGeodatabase_conversion(fc_path, gdb_path)

    return gdb_path
