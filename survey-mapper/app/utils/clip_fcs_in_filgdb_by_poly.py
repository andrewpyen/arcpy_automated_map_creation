# --- Inputs (edit these) ---
SRC_GDB = "C:\\Users\\Kim\\Documents\\repos\\geoinfo-processor\\backend\\source-data\\gdbs\\Sample_GDB.gdb"         # source file geodatabase 
OUT_GDB = "C:\\Users\\Kim\\Documents\\repos\\geoinfo-processor\\backend\\source-data\\gdbs\\Sample_Small_GDB.gdb" # will be created if it doesn't exist
BOUNDARY_FC_NAME = "OldDocBoundary"              # feature class name inside SRC_GDB
DEF_QUERY = "BOUNDARYNAMEID IN ('0520','0505','0504','0503','0514','0502','0512','0522','0511','0513','0521','0532','0530','0531','0529','0528','0081','0501','0519','0073','0523','0089','0510')"                  # your definition query (SQL where-clause)

# --- Script starts here ---
import arcpy
import os

arcpy.env.overwriteOutput = True
arcpy.env.workspace = SRC_GDB

def msg(m, level="INFO"):
    f = {"INFO": arcpy.AddMessage, "WARN": arcpy.AddWarning, "ERROR": arcpy.AddError}[level]
    f(m)

# Validate inputs
if not arcpy.Exists(SRC_GDB):
    raise arcpy.ExecuteError(f"Source GDB does not exist: {SRC_GDB}")

# Create output GDB if needed
out_folder = os.path.dirname(OUT_GDB)
out_name = os.path.basename(OUT_GDB)
if not arcpy.Exists(OUT_GDB):
    msg(f"Creating output geodatabase: {OUT_GDB}")
    arcpy.management.CreateFileGDB(out_folder, out_name)

# Paths
boundary_fc = os.path.join(SRC_GDB, BOUNDARY_FC_NAME)
if not arcpy.Exists(boundary_fc):
    raise arcpy.ExecuteError(f"Boundary feature class not found in source GDB: {BOUNDARY_FC_NAME}")

# Build a layer with the definition query (selection)
msg(f"Building boundary layer '{BOUNDARY_FC_NAME}' with query: {DEF_QUERY!r}")
boundary_lyr = "boundary_lyr"
arcpy.management.MakeFeatureLayer(boundary_fc, boundary_lyr, DEF_QUERY)

# Confirm there are boundary features to use
cnt = int(arcpy.management.GetCount(boundary_lyr)[0])
if cnt == 0:
    raise arcpy.ExecuteError("The definition query returned zero boundary features. Nothing to clip.")

# Copy selected boundary into output (as a record of what was used)
boundary_out = os.path.join(OUT_GDB, f"{BOUNDARY_FC_NAME}_selected")
if arcpy.Exists(boundary_out):
    arcpy.management.Delete(boundary_out)
msg(f"Exporting selected boundary to: {boundary_out}")
arcpy.management.CopyFeatures(boundary_lyr, boundary_out)

# Detect geometry type of boundary (Clip requires polygon for polygonal clip)
b_desc = arcpy.Describe(boundary_lyr)
if getattr(b_desc, "shapeType", "").lower() != "polygon":
    msg("WARNING: Boundary is not a polygon. Feature classes will be *selected by location* and exported, not geometrically clipped.", "WARN")
    do_clip = False
else:
    do_clip = True

# Helper: produce a safe output name (<= 160 chars, no invalids)
def safe_name(name):
    cleaned = name.replace(" ", "_").replace("-", "_")
    return cleaned[:160]

# Walk the GDB and process feature classes (including inside feature datasets)
processed = 0
failed = 0

msg("Scanning source GDB for feature classes …")
for dirpath, dirnames, filenames in arcpy.da.Walk(SRC_GDB, datatype="FeatureClass"):
    for fc in filenames:
        in_fc = os.path.join(dirpath, fc)

        # Skip the boundary FC itself (we already exported the selected features)
        if os.path.normcase(in_fc) == os.path.normcase(boundary_fc):
            continue

        # Build output name
        rel = os.path.relpath(in_fc, SRC_GDB).replace("\\", "_").replace("/", "_")
        out_name = safe_name(f"{rel}_clip")
        out_fc = os.path.join(OUT_GDB, out_name)

        # Get geometry type to avoid attempting to clip unsupported types
        try:
            gtype = arcpy.Describe(in_fc).shapeType.upper()
        except Exception:
            gtype = "UNKNOWN"

        try:
            if do_clip and gtype in ("POLYGON", "POLYLINE", "POINT", "MULTIPOINT", "MULTIPATCH"):
                # Prefer faster, robust PairwiseClip; fall back to Clip if not available
                try:
                    msg(f"PairwiseClip: {in_fc} -> {out_fc}")
                    arcpy.analysis.PairwiseClip(in_fc, boundary_lyr, out_fc)
                except Exception:
                    msg(f"PairwiseClip failed; trying Clip: {in_fc} -> {out_fc}", "WARN")
                    arcpy.analysis.Clip(in_fc, boundary_lyr, out_fc)
            else:
                # Non-polygon boundary case: select by location and export
                msg(f"Selecting by location & exporting (no geometric clip): {in_fc} -> {out_fc}")
                lyr = "work_lyr"
                arcpy.management.MakeFeatureLayer(in_fc, lyr)
                arcpy.management.SelectLayerByLocation(lyr, overlap_type="INTERSECT", select_features=boundary_lyr)
                arcpy.conversion.FeatureClassToFeatureClass(lyr, OUT_GDB, out_name)
                arcpy.management.Delete(lyr)

            processed += 1
        except Exception as ex:
            failed += 1
            msg(f"ERROR processing {in_fc}: {ex}", "ERROR")

msg(f"Done. Processed: {processed} | Failed: {failed}")
