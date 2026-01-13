import arcpy
import os
import openpyxl
from sys import argv
import json
import logging
from datetime import datetime

script_start_time = datetime.now()

log_folder = r"D:\LSA\logs"  
os.makedirs(log_folder, exist_ok=True)

with open('D:\Onedrive\OneDrive - Red Planet Consulting Pvt Ltd\Projects\LEAKSURVEY\ArcPy_code_review\walking_config.json', 'r') as json_file:
    config = json.load(json_file)

# Generate a unique log file name using timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_folder, f"log_walking_{timestamp}.txt")

# Create a logger
logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)  # Set the log level

# Create a file handler (so it creates a new file each run)
file_handler = logging.FileHandler(log_filename, mode="w")
file_handler.setLevel(logging.INFO)

# Create a log format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

print(f"New log file created: {log_filename}")


arcpy.env.overwriteOutput = True

exceln = arcpy.GetParameterAsText(0)
Excelfield = arcpy.GetParameterAsText(1)
GDB = arcpy.GetParameterAsText(2)
parent_directory = arcpy.GetParameterAsText(3)
logger.info(f"Parameters received:\n  Excel: {exceln}\n  Excel Field: {Excelfield}\n  GDB: {GDB}\n  Parent Directory: {parent_directory}")



LongServices = GDB +"\\LongServices"
Gridzones = GDB +"\\Gridzones"
MegaServices = GDB +"\\MegaServices"
Proposed = GDB +"\\Proposed"
Stub = GDB +"\\Stub"
Services = GDB +"\\Services"
Mains = GDB +"\\Mains"
NullRiser = GDB +"\\NullRiser"
Riser = GDB +"\\Riser"
InactiveRiser = GDB +"\\InactiveRiser"
LandbaseTextDimension = os.path.join(GDB, "LandbaseTextDimension")
MainAnnotation = os.path.join(GDB, "MainAnnotation")
ServiceAnnotation = os.path.join(GDB, "ServiceAnnotation")
LandbaseTextProposed = os.path.join(GDB, "LandbaseTextProposed")
LandbaseTextLotNum = os.path.join(GDB, "LandbaseTextLotNum")
LandbaseTextAddr = os.path.join(GDB, "LandbaseTextAddr")

#GDB
workspace1 = parent_directory

#set name GDB
gdb_name = "my_gdb.gdb"

#Create GDB
try:
    arcpy.CreateFileGDB_management(workspace1, gdb_name)
    logger.info("Created geodatabase")
except Exception as e:
    logger.error(f"Error creating geodatabase: {e}")
    arcpy.AddError(f"Error creating geodatabase: {e}")
    raise

#Check_Excel name
try:
    workbook = openpyxl.load_workbook(exceln)
    #sheet_name
    sheet_names = workbook.sheetnames
    logger.info(f"Excel sheets found: {sheet_names}")
except Exception as e:
    logger.error(f"Error reading Excel file {exceln}: {e}")
    arcpy.AddError(f"Error reading Excel file {exceln}: {e}")
    raise


# Print_Sheet_name
for sheet_name in sheet_names:

    if ' ' in sheet_name:
        sheet_name = "T_" + sheet_name + "$_"
        sheet_name = sheet_name.replace(" ", "_")
        print(sheet_name)
        logger.info(f"Processing sheet: {sheet_name}")
    else:
        sheet_name = sheet_name + "$"
        sheet_name = sheet_name.replace(" ", "_")
        print(sheet_name)
        logger.info(f"Processing sheet: {sheet_name}")
    #new_folder
    folder_name = sheet_name

    #path_newfolder
    folder_path = os.path.join(parent_directory, folder_name)

    #createfolder
    os.mkdir(folder_path)
    print(str(exceln) + "\\" + "\\" + str(sheet_name))
    logger.info(str(exceln) + "\\" + "\\" + str(sheet_name))

    def model():
        print ('Model Function Started')
        logger.info('Model Function Started')
        arcpy.env.overwriteOutput = True

        T_0001_0023_ = str(exceln) + "\\" + "\\" + str(sheet_name)
        try:
            #Add_Join
            gridzones_layer = arcpy.management.AddJoin(in_layer_or_view=Gridzones, in_field="GridZoneId", join_table=T_0001_0023_,
                                                    join_field=Excelfield, join_type="KEEP_COMMON")[0]
            logger.info(f"AddJoin successful for sheet: {sheet_name}")
            print ('Add Join Completed')
        except Exception as e:
            logger.error(f"Error in Add Join {sheet_name}:{e}")
            arcpy.AddError(f"Error in Add Join {sheet_name}: {e}")

        #Export Features
        gridzones_shp = folder_path + "\\Gridzones.shp"
        try:
            arcpy.conversion.ExportFeatures(in_features=gridzones_layer, out_features=gridzones_shp,
                                            field_mapping= config.get('fieldmapping', {}).get('gridzones_shp'))
            gridzones_shp_feature_count = int(arcpy.GetCount_management(gridzones_shp)[0])
            logger.info(f"Exported Gridzones shapefile to {gridzones_shp} with {gridzones_shp_feature_count} features.")
            arcpy.AddMessage(f"Exported Gridzones shapefile to {gridzones_shp} with {gridzones_shp_feature_count} features.")
            print (f'Add Join Completed And Features exported No of Features Exported:{gridzones_shp_feature_count}') 
        except Exception as e:
            logger.error(f"Error exporting Gridzones shapefile for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error exporting Gridzones shapefile for sheet {sheet_name}: {e}")
            print ('Add Join Completed And Features Not exported')
            raise
            
        
        #Clip
        longservices_clip = workspace1  + "\\" + gdb_name + "\\LongServices_Clip"
        try:
            arcpy.analysis.Clip(in_features=LongServices, clip_features=gridzones_shp,
                                out_feature_class=longservices_clip)
            longservices_clip_feature_count = int(arcpy.GetCount_management(longservices_clip)[0])
            logger.info(f"Clipped LongServices to {longservices_clip} with {longservices_clip_feature_count} features.")
            arcpy.AddMessage(f"Exported Gridzones shapefile to {longservices_clip} with {longservices_clip_feature_count} features.")
            print (f'Clipped LongServices to {longservices_clip} Completed And No of Features Exported:{gridzones_shp_feature_count}') 
        except Exception as e:
            logger.error(f"Error clipping LongServices for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping LongServices for sheet {sheet_name}: {e}")
            print('LongServices Clip Failed')
            raise
        try:
            longservices_clip_layer, lsc_count = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=longservices_clip, where_clause=config.get('clip', {}).get('longservices_clip_layer'))
            print(lsc_count)
            longservices_clip_layer_not_in, lsc_count_not_in = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=longservices_clip,
                where_clause=config.get('clip', {}).get('longservices_clip_layer_not_in'))
            print(lsc_count_not_in)
            logger.info("Selected LongServices layers based on MATERIAL attribute.")
            print('LongServices SelectLayerByAttribute Success')
        except Exception as e:
            logger.error(f"Error selecting attributes on LongServices for sheet {sheet_name}: {e}")
            print('LongServices SelectLayerByAttribute Failed')
            raise

        megaservice_clip = workspace1  + "\\" + gdb_name + "\\MegaService_Clip"
        try:
            arcpy.analysis.Clip(in_features=MegaServices, clip_features=gridzones_shp,
                            out_feature_class=megaservice_clip)
            mega_services_clip_feature_count = int(arcpy.GetCount_management(megaservice_clip)[0])
            logger.info(f"Clipped MegaServices to {megaservice_clip} with {mega_services_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped MegaServices to {megaservice_clip} with {mega_services_clip_feature_count} features.")
            print('Clip Success')
        except Exception as e:
            logger.error(f"Error clipping MegaServices for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping MegaServices for sheet {sheet_name}: {e}")
            print('Clip Failed')
            raise   

        try:
            megaservices_clip_layer, msc_count = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=megaservice_clip,
                where_clause=config.get('clip', {}).get('megaservices_clip_layer'))
            print(msc_count)
            megaservices_clip_layer_not_in, msc_count_not_in = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=megaservice_clip,
                where_clause=config.get('clip', {}).get('megaservices_clip_layer_not_in'))
            print(msc_count_not_in)
            logger.info("Selected MegaServices layers based on MATERIAL attribute.")
            print('SelectLayerByAttribute Success')  
        except Exception as e:
            logger.error(f"Error selecting attributes on MegaServices for sheet {sheet_name}: {e}")
            print('SelectLayerByAttribute failed')  
            raise

        stub_clip = workspace1 + "\\" + gdb_name + "\\Stub_Clip"
        try:
            arcpy.analysis.Clip(in_features=Stub, clip_features=gridzones_shp,
                                out_feature_class=stub_clip)
            stub_clip_feature_count = int(arcpy.GetCount_management(stub_clip)[0])
            logger.info(f"Clipped Stub to {stub_clip} with {stub_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped Stub to {stub_clip} with {stub_clip_feature_count} features.")
            print('Clip Success')
        except Exception as e:
            logger.error(f"Error clipping Stub for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping Stub for sheet {sheet_name}: {e}")
            print('Clip Failed')
            raise

        try:
            #Select Layer By Attribute(management)
            stub_clip_layer, stub_clip_count = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=stub_clip,
                where_clause= config.get('clip', {}).get('stub_clip_layer'))
            print(stub_clip_count)

            stub_clip_layer_not_in, stub_clip_count_not_in = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=stub_clip,
                where_clause=config.get('clip', {}).get('stub_clip_layer_not_in'))
            print(stub_clip_count_not_in)
            logger.info("Selected Stub layers based on PATROL_TYPE and MATERIAL attributes.")
            print('SelectLayerByAttribute Success')  
        except Exception as e:
            logger.error(f"Error selecting attributes on Stub for sheet {sheet_name}: {e}")
            print('SelectLayerByAttribute failed')  
            raise

        #Merge Stub into Services
        services_shp = folder_path + "\\Services.shp"
        try:
            arcpy.management.Merge(inputs=[longservices_clip_layer, megaservices_clip_layer, stub_clip_layer],
                                output=services_shp)
            services_shp_feature_count = int(arcpy.GetCount_management(services_shp)[0])
            logger.info(f"Merged layers into Services shapefile at {services_shp} with {services_shp_feature_count} features.")
            arcpy.AddMessage(f"Merged layers into Services shapefile at {services_shp} with {services_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error merging layers for Services in sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error merging layers for Services in sheet {sheet_name}: {e}")
            raise

        proposed_clip = workspace1 + "\\" + "\\" + gdb_name + "\\Proposed_Clip"
        try:
            arcpy.analysis.Clip(in_features=Proposed, clip_features=gridzones_shp,
                            out_feature_class=proposed_clip)
            proposed_clip_feature_count = int(arcpy.GetCount_management(proposed_clip)[0])
            logger.info(f"Clipped Proposed features to {proposed_clip} with {proposed_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped Proposed features to {proposed_clip} with {proposed_clip_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping Proposed features for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping Proposed features for sheet {sheet_name}: {e}")
            raise
        
        try:
            #Select Proposed features by attribute and export them
            proposed_clip_layer, pc_count = arcpy.management.SelectLayerByAttribute(
            in_layer_or_view=proposed_clip,
            where_clause=config.get('clip', {}).get('proposed_clip_layer'))

            print(pc_count)

            proposed_shp = folder_path + "\\Proposed.shp"
            arcpy.conversion.ExportFeatures(in_features=proposed_clip_layer, out_features=proposed_shp,
                                            field_mapping= config.get('fieldmapping', {}).get('proposed_shp'))

            proposed_clip_layer_not_in, pc_count_not_in = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=proposed_clip,
                where_clause=config.get('clip', {}).get('proposed_clip_layer_not_in'))
            print(pc_count_not_in)
            proposed_shp_feature_count = int(arcpy.GetCount_management(proposed_shp)[0])
            logger.info(f"Exported Proposed shapefile to {proposed_shp} with {proposed_shp_feature_count} features.")
            arcpy.AddMessage(f"Exported Proposed shapefile to {proposed_shp} with {proposed_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error processing Proposed features for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error processing Proposed features for sheet {sheet_name}: {e}")

        stub_clip = workspace1 + "\\" + "\\" + gdb_name + "\\InactiveRiser_Clip"
        try:
            arcpy.analysis.Clip(in_features=InactiveRiser, clip_features=gridzones_shp,
                            out_feature_class=stub_clip)
            inactive_riser_clip_feature_count = int(arcpy.GetCount_management(stub_clip)[0])
            logger.info(f"Clipped InactiveRiser to {stub_clip} with {inactive_riser_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped InactiveRiser to {stub_clip} with {inactive_riser_clip_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping InactiveRiser for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping InactiveRiser for sheet {sheet_name}: {e}")
            raise

        # # Process: Select Layer By Attribute (12) (Select Layer By Attribute) (management)
        # inactiveriser_clip_layer, iar_clip_count = arcpy.management.SelectLayerByAttribute(
        #     in_layer_or_view=stub_clip,
        #     where_clause=config.get('clip', {}).get('inactiveriser_clip_layer'))
        #
        # inactiveriser_clip_layer_not_in, iar_clip_count_not_in = arcpy.management.SelectLayerByAttribute(
        #     in_layer_or_view=stub_clip,
        #     where_clause="config.get('clip', {}).get('inactiveriser_clip_layer_not_in'))

        #NullRiser
        proposed_clip = workspace1 + "\\" + "\\" + gdb_name + "\\NullRiser_Clip"
        try:
            arcpy.analysis.Clip(in_features=NullRiser, clip_features=gridzones_shp,
                            out_feature_class=proposed_clip)
            null_riser_clip_feature_count = int(arcpy.GetCount_management(proposed_clip)[0])
            logger.info(f"Clipped NullRiser to {proposed_clip} with {null_riser_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped NullRiser to {proposed_clip} with {null_riser_clip_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping NullRiser for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping NullRiser for sheet {sheet_name}: {e}")
            raise

        riser_shp = folder_path + "\\Riser.shp"
        try:
            arcpy.management.Merge(inputs=[proposed_clip, stub_clip], output=riser_shp,
                               field_mappings=config.get('fieldmapping', {}).get('riser_shp'))
            riser_shp_feature_count = int(arcpy.GetCount_management(riser_shp)[0])
            logger.info(f"Merged NullRiser and InactiveRiser clips into Riser shapefile at {riser_shp} with {riser_shp_feature_count} features.")
            arcpy.AddMessage(f"Merged NullRiser and InactiveRiser clips into Riser shapefile at {riser_shp} with {riser_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error merging Riser clips for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error merging Riser clips for sheet {sheet_name}: {e}")
            raise
        
        services_clip = workspace1 + "\\" + "\\" + gdb_name + "\\Services_Clip"
        try:
            arcpy.analysis.Clip(in_features=Services, clip_features=gridzones_shp,
                                out_feature_class=services_clip)
            services_clip_feature_count = int(arcpy.GetCount_management(services_clip)[0])
            logger.info(f"Clipped Services to {services_clip} with {services_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped Services to {services_clip} with {services_clip_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping Services for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping Services for sheet {sheet_name}: {e}")
            raise

        mains_clip = workspace1 + "\\" + "\\" + gdb_name + "\\Mains_Clip"
        try:
            arcpy.analysis.Clip(in_features=Mains, clip_features=gridzones_shp,
                            out_feature_class=mains_clip)
            mains_clip_feature_count = int(arcpy.GetCount_management(mains_clip)[0])
            logger.info(f"Clipped Mains to {mains_clip} with {mains_clip_feature_count} features.")
            arcpy.AddMessage(f"Clipped Mains to {mains_clip} with {mains_clip_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping Mains for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping Mains for sheet {sheet_name}: {e}")
            raise

        #NILines
        nilines_shp = folder_path + "\\NILines.shp"
        try:
            arcpy.management.Merge(
                inputs=[mains_clip, services_clip,
                        proposed_clip_layer_not_in, stub_clip_layer_not_in, megaservices_clip_layer_not_in,longservices_clip_layer_not_in],
                output=nilines_shp,
                field_mappings=config.get('fieldmapping', {}).get('nilines_shp'), add_source="NO_SOURCE_INFO")
            nilines_shp_feature_count = int(arcpy.GetCount_management(nilines_shp)[0])
            logger.info(f"Merged NILines shapefile at {nilines_shp} with {nilines_shp_feature_count} features.")
            arcpy.AddMessage(f"Merged NILines shapefile at {nilines_shp} with {nilines_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error merging NILines for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error merging NILines for sheet {sheet_name}: {e}")
            raise

        #NIPoints
        nipoints_shp = folder_path + "\\NIPoints.shp"
        try:
            arcpy.analysis.Clip(in_features=Riser, clip_features=gridzones_shp,
                            out_feature_class=nipoints_shp)
                            # field_mapping=config.get('fieldmapping', {}).get('nipoints_shp'))
            nipoints_shp_feature_count = int(arcpy.GetCount_management(nipoints_shp)[0])
            logger.info(f"Clipped Riser to create NIPoints shapefile at {nipoints_shp} with {nipoints_shp_feature_count} features.")
            arcpy.AddMessage(f"Clipped Riser to create NIPoints shapefile at {nipoints_shp} with {nipoints_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error clipping Riser for NIPoints in sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error clipping Riser for NIPoints in sheet {sheet_name}: {e}")
            raise
        
        try:
            #Annotations
            landbase_text_dimension_shp = os.path.join(workspace1, gdb_name, "LandbaseTextDimension_Clip")
            arcpy.analysis.Clip(LandbaseTextDimension, gridzones_shp, landbase_text_dimension_shp)
            logger.info(f"Clipped LandbaseTextDimension to {landbase_text_dimension_shp}")

            main_annotation_shp = os.path.join(workspace1, gdb_name, "MainAnnotation_Clip")
            arcpy.analysis.Clip(MainAnnotation, gridzones_shp, main_annotation_shp)
            logger.info(f"Clipped MainAnnotation to {main_annotation_shp}")

            service_annotation_shp = os.path.join(workspace1, gdb_name, "ServiceAnnotation_Clip")
            arcpy.analysis.Clip(ServiceAnnotation, gridzones_shp, service_annotation_shp)
            logger.info(f"Clipped ServiceAnnotation to {service_annotation_shp}")

            landbase_text_proposed_shp = os.path.join(workspace1, gdb_name, "LandbaseTextProposed_Clip")
            arcpy.analysis.Clip(LandbaseTextProposed, gridzones_shp, landbase_text_proposed_shp)
            logger.info(f"Clipped LandbaseTextProposed to {landbase_text_proposed_shp}")

            landbase_text_lot_num_shp = os.path.join(workspace1, gdb_name, "LandbaseTextLotNum_Clip")
            arcpy.analysis.Clip(LandbaseTextLotNum, gridzones_shp, landbase_text_lot_num_shp)
            logger.info(f"Clipped LandbaseTextLotNum to {landbase_text_lot_num_shp}")

            landbase_text_addr_shp = os.path.join(workspace1, gdb_name, "LandbaseTextAddr_Clip")
            arcpy.analysis.Clip(LandbaseTextAddr, gridzones_shp, landbase_text_addr_shp)
            logger.info(f"Clipped LandbaseTextAddr to {landbase_text_addr_shp}")

            annotation_shp = os.path.join(folder_path, "Annotation.shp")
            merged_features = arcpy.management.Merge(
                [landbase_text_dimension_shp, main_annotation_shp, service_annotation_shp,
                landbase_text_proposed_shp, landbase_text_lot_num_shp, landbase_text_addr_shp])
            arcpy.conversion.ExportFeatures(merged_features, annotation_shp)
            annotation_shp_feature_count = int(arcpy.GetCount_management(annotation_shp)[0])
            logger.info(f"Exported merged Annotations shapefile to {annotation_shp} with {annotation_shp_feature_count} features.")
            arcpy.AddMessage(f"Exported merged Annotations shapefile to {annotation_shp} with {annotation_shp_feature_count} features.")
        except Exception as e:
            logger.error(f"Error processing Annotations for sheet {sheet_name}: {e}")
            arcpy.AddMessage(f"Error processing Annotations for sheet {sheet_name}: {e}")
            raise


       # Compare shapefiles in folder with feature classes in GDB and export missing ones
        try:
            # Get the list of files in the folder
            file_list = os.listdir(folder_path)

            # Extract the names of shapefiles (without extension)
            shp_files = [os.path.splitext(file_name)[0] for file_name in file_list if file_name.lower().endswith(".shp")]

            # Set the workspace to the geodatabase
            arcpy.env.workspace = GDB

            # Get a list of feature classes in the geodatabase
            feature_classes = arcpy.ListFeatureClasses()

            # Find the non-similar items (shapefiles not present as feature classes)
            non_similar_files = set(feature_classes) - set(shp_files)
            values = ["Address", "BusinessDistricts", "PropertyLine", "ROWLine", "MobileHomeParks", "ControllableFitting", 
                    "CPTestPoint", "GasLeakRepair", "GasValve", "HouseNbrAnno", "NonControllableFitting", "RegulatorStation",
                    "GasLamp", "AMIndicationLocation", "AMIndicationPolygon", "AMCoveragePolygon", "NIPoints", "NILines", "NIPolygons"
            ]

            for item in non_similar_files:
                for v in values:
                    if v == item:
                        print(v)
                        # Process: Export Features (3) (Export Features) (conversion)
                        non_assets_lyrs = GDB + "\\" + v

                        # Process: Clip (2) (Clip) (analysis)
                        non_assets_lyrs_shp = folder_path + "\\" + v + ".shp"
                        arcpy.analysis.Clip(in_features=non_assets_lyrs, clip_features=gridzones_shp,
                                            out_feature_class=non_assets_lyrs_shp)
                        logger.info(f"Exported clipped feature class for {item} to {non_assets_lyrs_shp}")
        except Exception as e:
            logger.error(f"Error processing additional feature classes for sheet {sheet_name}: {e}")
            raise

    if __name__ == '__main__':
        # Global Environment settings
        with arcpy.EnvManager(scratchWorkspace=workspace1 + "\\" + "\\" + gdb_name + "", workspace=workspace1 + "\\" + "\\" + gdb_name + ""):
            model()
