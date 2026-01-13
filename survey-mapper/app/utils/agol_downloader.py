import os
import arcpy
from typing import List

def download_features_to_gdb(urls: List[str], output_gdb_path: str):
    """
    Downloads Esri-hosted feature layers (ArcGIS Online) into a local file geodatabase.

    Args:
        urls (List[str]): List of feature layer URLs.
        output_gdb_path (str): Path to the file geodatabase to write into.

    Returns:
        None
    """
    if not arcpy.Exists(output_gdb_path):
        parent_dir = os.path.dirname(output_gdb_path)
        gdb_name = os.path.basename(output_gdb_path)
        arcpy.CreateFileGDB_management(parent_dir, gdb_name)

    for url in urls:
        try:
            layer_name = url.rstrip('/').split('/')[-1]
            out_fc = os.path.join(output_gdb_path, f"{layer_name}_downloaded")
            arcpy.FeatureClassToFeatureClass_conversion(in_features=url, out_path=output_gdb_path, out_name=layer_name)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
