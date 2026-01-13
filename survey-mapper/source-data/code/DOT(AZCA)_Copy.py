import arcpy
import os
import openpyxl
import json
import logging
from datetime import datetime

# --- Setup Logging ---
script_start_time = datetime.now()
log_folder = r"D:\LSA\DOT_logs"  # Hardcoded path as per user preference
os.makedirs(log_folder, exist_ok=True)

# Generate a unique log file name using timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_folder, f"log_dot_{timestamp}.txt")

# Create a logger
logger = logging.getLogger("DOTLogger")
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler(log_filename, mode="w")
file_handler.setLevel(logging.INFO)

# Create a log format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info(f"New log file created: {log_filename}")

# --- Load Configuration ---
with open(os.path.join(arcpy.GetParameterAsText(3), "dot_config.json"), 'r') as json_file:
    config = json.load(json_file)

# --- Input Parameters ---
arcpy.env.overwriteOutput = True
exceln = arcpy.GetParameterAsText(0)
Excelfield = arcpy.GetParameterAsText(1)
GDB = arcpy.GetParameterAsText(2)
parent_directory = arcpy.GetParameterAsText(3)
logger.info(f"Parameters received:\n  Excel: {exceln}\n  Excel Field: {Excelfield}\n  GDB: {GDB}\n  Parent Directory: {parent_directory}")

# --- Define Feature Class Paths ---
Gridzones = os.path.join(GDB, "Gridzones")
Mains = os.path.join(GDB, "Mains")
Riser = os.path.join(GDB, "Riser")
InactiveRiser = os.path.join(GDB, "InactiveRiser")
NullRiser = os.path.join(GDB, "NullRiser")
LongServices = os.path.join(GDB, "LongServices")
MegaServices = os.path.join(GDB, "MegaServices")
Proposed = os.path.join(GDB, "Proposed")
Services = os.path.join(GDB, "Services")
LandbaseTextDimension = os.path.join(GDB, "LandbaseTextDimension")
MainAnnotation = os.path.join(GDB, "MainAnnotation")
ServiceAnnotation = os.path.join(GDB, "ServiceAnnotation")
LandbaseTextProposed = os.path.join(GDB, "LandbaseTextProposed")
LandbaseTextLotNum = os.path.join(GDB, "LandbaseTextLotNum")
LandbaseTextAddr = os.path.join(GDB, "LandbaseTextAddr")

# --- Create New File Geodatabase ---
workspace1 = parent_directory
gdb_name = "my_gdb.gdb"
output_gdb = os.path.join(workspace1, gdb_name)
try:
    arcpy.CreateFileGDB_management(workspace1, gdb_name)
    logger.info(f"Created geodatabase: {output_gdb}")
except Exception as e:
    logger.error(f"Error creating geodatabase: {e}")
    arcpy.AddError(f"Error creating geodatabase: {e}")
    raise

# --- Load Excel Workbook and Get Sheet Names ---
try:
    workbook = openpyxl.load_workbook(exceln)
    sheet_names = workbook.sheetnames
    logger.info(f"Excel sheets found: {sheet_names}")
except Exception as e:
    logger.error(f"Error reading Excel file {exceln}: {e}")
    arcpy.AddError(f"Error reading Excel file {exceln}: {e}")
    raise

# --- Process Each Sheet ---
for sheet_name in sheet_names:
    try:
        # Sanitize sheet name
        if " " in sheet_name:
            sanitized_sheet = f"T_{sheet_name}$_"
            sanitized_sheet = sanitized_sheet.replace(" ", "_")
        else:
            sanitized_sheet = f"{sheet_name}$"
            sanitized_sheet = sanitized_sheet.replace(" ", "_")
        logger.info(f"Processing sheet: {sanitized_sheet}")
        arcpy.AddMessage(f"Processing sheet: {sanitized_sheet}")

        # Create output folder for this sheet
        folder_path = os.path.join(parent_directory, sanitized_sheet)
        os.mkdir(folder_path)
        logger.info(f"Created folder: {folder_path}")

        # Create Excel table reference
        excel_table = f"{exceln}\\\\{sanitized_sheet}"
        logger.info(f"Using Excel table: {excel_table}")

        def Model():
            arcpy.env.overwriteOutput = True
            logger.info("Model function started")

            # 1. Add Join: Join Gridzones with the Excel table
            try:
                gridzones_layer = arcpy.management.AddJoin(
                    in_layer_or_view=Gridzones,
                    in_field="GridZoneId",
                    join_table=excel_table,
                    join_field=Excelfield,
                    join_type="KEEP_COMMON"
                )[0]
                logger.info(f"AddJoin successful for sheet: {sanitized_sheet}")
            except Exception as e:
                logger.error(f"Error in AddJoin for sheet {sanitized_sheet}: {e}")
                raise

            # 2. Export Gridzones shapefile
            gridzones_shp = os.path.join(folder_path, "Gridzones.shp")
            try:
                arcpy.conversion.ExportFeatures(
                    in_features=gridzones_layer,
                    out_features=gridzones_shp,
                    field_mapping=config.get('fieldmapping', {}).get('gridzones_shp')
                )
                feature_count = int(arcpy.GetCount_management(gridzones_shp)[0])
                logger.info(f"Exported Gridzones shapefile to {gridzones_shp} with {feature_count} features")
                arcpy.AddMessage(f"Exported Gridzones shapefile to {gridzones_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error exporting Gridzones shapefile for sheet {sanitized_sheet}: {e}")
                raise

            # 3. Select Mains by attribute
            try:
                mains_layer, mains_count = arcpy.management.SelectLayerByAttribute(
                    in_layer_or_view=Mains,
                    where_clause=config.get('clip', {}).get('mains_layer')
                )
                logger.info(f"Selected Mains with {mains_count} features")
            except Exception as e:
                logger.error(f"Error selecting Mains for sheet {sanitized_sheet}: {e}")
                raise

            # 4. Clip Mains
            mains_clip = os.path.join(output_gdb, "Mains_Clip")
            try:
                arcpy.analysis.Clip(mains_layer, gridzones_shp, mains_clip)
                feature_count = int(arcpy.GetCount_management(mains_clip)[0])
                logger.info(f"Clipped Mains to {mains_clip} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error clipping Mains for sheet {sanitized_sheet}: {e}")
                raise

            # 5. Clip NullRiser
            nullriser_clip = os.path.join(output_gdb, "NullRiser_Clip")
            try:
                arcpy.analysis.Clip(NullRiser, gridzones_shp, nullriser_clip)
                feature_count = int(arcpy.GetCount_management(nullriser_clip)[0])
                logger.info(f"Clipped NullRiser to {nullriser_clip} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error clipping NullRiser for sheet {sanitized_sheet}: {e}")
                raise

            # 6. Clip InactiveRiser
            inactiveriser_clip = os.path.join(output_gdb, "InactiveRiser_Clip")
            try:
                arcpy.analysis.Clip(InactiveRiser, gridzones_shp, inactiveriser_clip)
                feature_count = int(arcpy.GetCount_management(inactiveriser_clip)[0])
                logger.info(f"Clipped InactiveRiser to {inactiveriser_clip} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error clipping InactiveRiser for sheet {sanitized_sheet}: {e}")
                raise

            # 7. Merge to NIPoints
            nipoints_shp = os.path.join(folder_path, "NIPoints.shp")
            try:
                arcpy.management.Merge(
                    inputs=[nullriser_clip, inactiveriser_clip],
                    output=nipoints_shp,
                    field_mappings=config.get('fieldmapping', {}).get('nipoints_shp')
                )
                feature_count = int(arcpy.GetCount_management(nipoints_shp)[0])
                logger.info(f"Merged NIPoints shapefile to {nipoints_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error merging NIPoints for sheet {sanitized_sheet}: {e}")
                raise

            # 8. Clip LongServices
            longservices_clip = os.path.join(output_gdb, "LongServices_Clip")
            try:
                arcpy.analysis.Clip(LongServices, gridzones_shp, longservices_clip)
                feature_count = int(arcpy.GetCount_management(longservices_clip)[0])
                logger.info(f"Clipped LongServices to {longservices_clip} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error clipping LongServices for sheet {sanitized_sheet}: {e}")
                raise

            # 9. Clip MegaServices
            megaservices_clip = os.path.join(output_gdb, "MegaServices_Clip")
            try:
                arcpy.analysis.Clip(MegaServices, gridzones_shp, megaservices_clip)
                feature_count = int(arcpy.GetCount_management(megaservices_clip)[0])
                logger.info(f"Clipped MegaServices to {megaservices_clip} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error clipping MegaServices for sheet {sanitized_sheet}: {e}")
                raise

            # 10. Merge to NILines
            nilines_shp = os.path.join(folder_path, "NILines.shp")
            try:
                arcpy.management.Merge(
                    inputs=[longservices_clip, megaservices_clip],
                    output=nilines_shp,
                    field_mappings=config.get('fieldmapping', {}).get('nilines_shp')
                )
                feature_count = int(arcpy.GetCount_management(nilines_shp)[0])
                logger.info(f"Merged NILines shapefile to {nilines_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error merging NILines for sheet {sanitized_sheet}: {e}")
                raise

            # 11. Select Proposed by attribute
            try:
                proposed_layer, proposed_count = arcpy.management.SelectLayerByAttribute(
                    in_layer_or_view=Proposed,
                    where_clause=config.get('clip', {}).get('proposed_layer')
                )
                logger.info(f"Selected Proposed with {proposed_count} features")
            except Exception as e:
                logger.error(f"Error selecting Proposed for sheet {sanitized_sheet}: {e}")
                raise

            # 12. Clip Proposed and merge with Mains
            proposed_shp = os.path.join(folder_path, "Mains.shp")
            try:
                proposed_clip = os.path.join(output_gdb, "Proposed_Clip")
                arcpy.analysis.Clip(proposed_layer, gridzones_shp, proposed_clip)
                feature_count = int(arcpy.GetCount_management(proposed_clip)[0])
                logger.info(f"Clipped Proposed to {proposed_clip} with {feature_count} features")

                arcpy.management.Merge(
                    inputs=[proposed_clip, mains_clip],
                    output=proposed_shp,
                    field_mappings=config.get('fieldmapping', {}).get('proposed_shp')
                )
                feature_count = int(arcpy.GetCount_management(proposed_shp)[0])
                logger.info(f"Merged Proposed and Mains to {proposed_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error processing Proposed shapefile for sheet {sanitized_sheet}: {e}")
                raise

            # 13. Process Annotations
            try:
                landbase_text_dimension_clip = os.path.join(output_gdb, "LandbaseTextDimension_Clip")
                arcpy.analysis.Clip(LandbaseTextDimension, gridzones_shp, landbase_text_dimension_clip)
                feature_count = int(arcpy.GetCount_management(landbase_text_dimension_clip)[0])
                logger.info(f"Clipped LandbaseTextDimension to {landbase_text_dimension_clip} with {feature_count} features")

                main_annotation_clip = os.path.join(output_gdb, "MainAnnotation_Clip")
                arcpy.analysis.Clip(MainAnnotation, gridzones_shp, main_annotation_clip)
                feature_count = int(arcpy.GetCount_management(main_annotation_clip)[0])
                logger.info(f"Clipped MainAnnotation to {main_annotation_clip} with {feature_count} features")

                service_annotation_clip = os.path.join(output_gdb, "ServiceAnnotation_Clip")
                arcpy.analysis.Clip(ServiceAnnotation, gridzones_shp, service_annotation_clip)
                feature_count = int(arcpy.GetCount_management(service_annotation_clip)[0])
                logger.info(f"Clipped ServiceAnnotation to {service_annotation_clip} with {feature_count} features")

                landbase_text_proposed_clip = os.path.join(output_gdb, "LandbaseTextProposed_Clip")
                arcpy.analysis.Clip(LandbaseTextProposed, gridzones_shp, landbase_text_proposed_clip)
                feature_count = int(arcpy.GetCount_management(landbase_text_proposed_clip)[0])
                logger.info(f"Clipped LandbaseTextProposed to {landbase_text_proposed_clip} with {feature_count} features")

                landbase_text_lot_num_clip = os.path.join(output_gdb, "LandbaseTextLotNum_Clip")
                arcpy.analysis.Clip(LandbaseTextLotNum, gridzones_shp, landbase_text_lot_num_clip)
                feature_count = int(arcpy.GetCount_management(landbase_text_lot_num_clip)[0])
                logger.info(f"Clipped LandbaseTextLotNum to {landbase_text_lot_num_clip} with {feature_count} features")

                landbase_text_addr_clip = os.path.join(output_gdb, "LandbaseTextAddr_Clip")
                arcpy.analysis.Clip(LandbaseTextAddr, gridzones_shp, landbase_text_addr_clip)
                feature_count = int(arcpy.GetCount_management(landbase_text_addr_clip)[0])
                logger.info(f"Clipped LandbaseTextAddr to {landbase_text_addr_clip} with {feature_count} features")

                annotation_shp = os.path.join(folder_path, "Annotation.shp")
                merged_features = arcpy.management.Merge([
                    landbase_text_dimension_clip, main_annotation_clip, service_annotation_clip,
                    landbase_text_proposed_clip, landbase_text_lot_num_clip, landbase_text_addr_clip
                ])
                arcpy.conversion.ExportFeatures(merged_features, annotation_shp)
                feature_count = int(arcpy.GetCount_management(annotation_shp)[0])
                logger.info(f"Exported merged Annotations shapefile to {annotation_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error processing Annotations for sheet {sanitized_sheet}: {e}")
                raise

            # 14. Export additional feature classes
            try:
                file_list = os.listdir(folder_path)
                shp_files = [os.path.splitext(f)[0] for f in file_list if f.lower().endswith(".shp")]
                arcpy.env.workspace = GDB
                feature_classes = arcpy.ListFeatureClasses()
                non_similar_files = set(feature_classes) - set(shp_files)
                values = [
                    "Mains", "Riser", "Services", "Address", "BusinessDistricts", "Stub",
                    "PropertyLine", "ROWLine", "MobileHomeParks", "ControllableFitting",
                    "CPTestPoint", "GasLeakRepair", "GasValve", "HouseNbrAnno",
                    "NonControllableFitting", "RegulatorStation", "GasLamp",
                    "AMIndicationLocation", "AMIndicationPolygon", "AMCoveragePolygon",
                    "NIPolygons"
                ]
                for item in non_similar_files:
                    if item in values:
                        fc_path = os.path.join(GDB, item)
                        output_shp = os.path.join(folder_path, f"{item}.shp")
                        arcpy.analysis.Clip(fc_path, gridzones_shp, output_shp)
                        feature_count = int(arcpy.GetCount_management(output_shp)[0])
                        logger.info(f"Exported clipped feature class {item} to {output_shp} with {feature_count} features")
            except Exception as e:
                logger.error(f"Error processing additional feature classes for sheet {sanitized_sheet}: {e}")
                raise

        # Run the Model function within an ArcPy environment manager
        if __name__ == '__main__':
            with arcpy.EnvManager(scratchWorkspace=output_gdb, workspace=output_gdb):
                Model()

        logger.info(f"Completed processing for sheet: {sanitized_sheet}")
        arcpy.AddMessage(f"Completed processing for sheet: {sanitized_sheet}")
    except Exception as e:
        logger.error(f"Error processing sheet {sheet_name}: {e}")
        arcpy.AddError(f"Error processing sheet {sheet_name}: {e}")

logger.info("Script completed")
arcpy.AddMessage("Script completed")