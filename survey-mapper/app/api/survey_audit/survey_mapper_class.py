import os
import logging
import openpyxl
import arcpy
import tempfile
import shutil
import re
import pandas as pd
from typing import Any, List, Optional, Dict, Set, Tuple, Union
from pathlib import Path
from app.api.survey_audit.shpToFeatureCollection_V1 import RecursiveExportFeatureCollection  # adjust import path as needed
from app.api.survey_audit.clip_counter import ClipCounter
from app.utils import helpers

def _safe_run_label(s: str, max_len: int = 80) -> str:
    s = (s or "").strip() or "grid_clip"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("._-")
    return s[:max_len]
class SurveyMapper:
    def __init__(self, 
            gdb_path: str, 
            parent_dir: str, 
            gridzone_excel_path: str,
            logger: logging.Logger,
            alternate_name_df: Optional[Union[pd.DataFrame, None]] = None,
            config_dict: Optional[Dict[str, Any]] = None,
            division_code: Optional[str] = None 
        ) -> None:
        """
        Initializes the RecursiveExportFeatureCollection class with paths to input data and configuration settings.

        Args:
            gdb_path (str): Full path to the source file geodatabase containing spatial feature classes.
            parent_dir (str): Parent directory where outputs such as feature collections or logs will be written.
            gridzone_excel_path (str): Path to the Excel file containing gridzone data.
            alternate_name_df (Optional[pd.DataFrame]): DataFrame containing alternative names for asset types

        Attributes:
            asset_lookup (dict): A dictionary that will be populated with alternative names or mappings for asset types.
        """
        self.gdb_path: str = gdb_path
        self.parent_dir: str = os.path.abspath(parent_dir)
        self.gridzone_excel_path: str = gridzone_excel_path
        self.alternate_name_df: pd.DataFrame | None = alternate_name_df
        self.division_code: Optional[str] = division_code

        if config_dict is not None:
            self._config = config_dict
        else:
            raise ValueError("Either config_dict or config_path must be provided.")

        # This needs to be configurable as a SQL server db connection to a table or to an excel table
        self.join_excel_field_name: str = self._config['gridzones']['join_excel_field_name']
        self.alternate_name_field: str = self._config['lutassettypes']['lutassettypes_new_name_field']
        self.alternate_name_db_name: str = self._config['lutassettypes']['source_sql_db_name']
        self.alternate_names_is_excel: bool = True if self._config['lutassettypes']['source_type'] == 'excel' else False

        # Excel-based alternate names and attribute queries
        self.alternate_name_map: Dict[str, Any] = {}

        # Logging setup
        log_folder = os.path.join(parent_dir, "logs")
        os.makedirs(log_folder, exist_ok=True)
        self.logger = logger or logging.getLogger(f"survey_mapper_tool.default")

    def _norm_name(self, s: Optional[str]) -> str:
        """Case-insensitive, trimmed name normalization for matching."""
        return (s or "").strip().lower()

    def _filter_by_division(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter feature_classes_to_clip for the active division using include/exclude lists.
        Matching checks both feature_class_name_source and shapefile_name_target.

        Rules:
        - If include_feature_classes is non-empty: start from ONLY those included.
        - Then drop anything that appears in exclude_feature_classes.
        - If include is empty or missing: start from all items, then drop excludes.

        Names are matched case-insensitively.
        """
        div = (self.division_code or "").strip().upper()
        if not div:
            # No division set, keep original list
            return items

        rules_list = self._config.get("division_feature_classes", []) or self._config.get("division_feature_classes".replace("i","i"))  # keep as-is if already correct
        rules = next((r for r in rules_list if (r.get("division") or "").strip().upper() == div), None)
        if not rules:
            # No rules for this division
            self.logger.info(f"No division rules found for '{div}'. Using all configured feature classes.")
            return items

        include_raw = rules.get("include_feature_classes") or []
        exclude_raw = rules.get("exclude_feature_classes") or []

        include = {self._norm_name(x) for x in include_raw if isinstance(x, str)}
        exclude = {self._norm_name(x) for x in exclude_raw if isinstance(x, str)}

        def names_for(fc: Dict[str, Any]) -> set[str]:
            return {
                self._norm_name(fc.get("feature_class_name_source")),
                self._norm_name(fc.get("shapefile_name_target"))
            }

        # Build the starting pool
        if include:
            pool = [fc for fc in items if names_for(fc) & include]
        else:
            pool = list(items)

        # Apply excludes
        filtered = [fc for fc in pool if not (names_for(fc) & exclude)]

        kept = [fc.get("feature_class_name_source") or fc.get("shapefile_name_target") for fc in filtered]
        self.logger.info(
            f"Division '{div}' filter applied. include={sorted(include) or 'ALL'}, exclude={sorted(exclude) or 'NONE'}. "
            f"Kept {len(filtered)} of {len(items)}: {sorted({self._norm_name(x) for x in kept})}"
        )
        return filtered

    def _generate_alternate_name_map(self) -> Dict[str, Any]:
        """
        Generates a mapping of asset names to their alternative names or geometry types.

        Args:
            None
        Returns:
            dict: A dictionary mapping asset names to their alternative names or geometry types.
        """
        self.alternate_name_map = {}
        if self.alternate_name_df is not None and not self.alternate_name_df.empty:

            if {
                "SourceDataName",
                "PreClipAttributeQuery",
                "IntermediateClipFilterName",
                "IntermediatePostClipFilterName",
                "PostClipAttributeQuery",
                "IntermediateMergeClipName",
                "OutputName",
                "GeometryType_Corrected",
                "IsAnnotationLayer",
                "IncludeInFinalResult"
                }.issubset(self.alternate_name_df.columns):

                self.alternate_name_map = dict(
                        SourceDataName=self.alternate_name_df["SourceDataName"], 
                        PreClipAttributeQuery=self.alternate_name_df["PreClipAttributeQuery"], 
                        IntermediateClipFilterName=self.alternate_name_df['IntermediateClipFilterName'],
                        IntermediatePostClipFilterName=self.alternate_name_df['IntermediatePostClipFilterName'],
                        PostClipAttributeQuery=self.alternate_name_df['PostClipAttributeQuery'],
                        IntermediateMergeClipName=self.alternate_name_df['IntermediateMergeClipName'],
                        OutputName=self.alternate_name_df['OutputName'],
                        GeometryType_Corrected=self.alternate_name_df['GeometryType_Corrected'],
                        IsAnnotationLayer=self.alternate_name_df['IsAnnotationLayer'],
                        IncludeInFinalResult=self.alternate_name_df['IncludeInFinalResult']
                )
                self.logger.info(f"Alternate name map built with {len(self.alternate_name_map)} entries.")
            else:
                self.logger.warning("self.alternate_name_df does not contain one or more of the required columns: " \
                "'SourceDataName', 'PreClipAttributeQuery', 'IntermediateClipFilterName', 'IntermediatePostClipFilterName', " \
                "'PostClipAttributeQuery', 'IntermediateMergeClipName', 'OutputName', 'IsAnnotationLayer', or 'IncludeInFinalResult'")

        return self.alternate_name_map
  

    def _is_annotation_fc(self, fc_path: str, recorded_as_anno: bool) -> Dict[str, bool]:
        """
        Determines if a feature class is an annotation feature class or if it's configured as an annotation layer.

        Args:
            fc_path (str): Full path to the feature class.

        Returns:
            bool: True if the feature class is an annotation type and is configered 
            as an annotation layer in the key 'isAnnotationLayer', False otherwise.
        """
        try:
            desc = arcpy.Describe(fc_path)
            return {
                "feature_class_is_annotation" : getattr(desc, "featureType", "").lower() == "annotation",
                "is_configured_as_annotation": recorded_as_anno
            }
        except Exception as e:
            self.logger.warning(f"Could not determine if {fc_path} is annotation: {e}")
            return {
                "feature_class_is_annotation" : False,
                "is_configured_as_annotation": recorded_as_anno
            }

    def _process_post_clip_attribute_query(self, per_sheet_gdb_path, post_clip_attribute_query):
        # Get ID and Source Layer to Get IDs from
        source_id_field, source_layer = post_clip_attribute_query.split(',')

        output_source_path = os.path.join(per_sheet_gdb_path, source_layer)

        # Get all SERVICEOBJECTSWGUIDs in table InactiveRisers
        lyr = f"{source_layer}_lyr"
        layer = arcpy.MakeFeatureLayer_management(output_source_path, lyr)  # or a feature layer name

        # Collect unique non-null values from the field
        values = {
            row[0]
            for row in arcpy.da.SearchCursor(layer, [source_id_field])
            if row[0] is not None
        }

        if not values:
            return "()"

        # If values are text (GUIDs, strings) they must be quoted for SQL
        sample = next(iter(values))
        if isinstance(sample, str):
            value_str = ", ".join("'{}'".format(v.replace("'", "''")) for v in values)
        else:
            value_str = ", ".join(str(v) for v in values)

        arcpy.Delete_management(lyr)

        return f"({source_id_field} IN ({value_str}))"
        
    def _existsInFileGdb(self, gdb_path, feature_class_name: str) -> bool:
        fc_path = os.path.join(gdb_path, feature_class_name)
        return arcpy.Exists(fc_path)
    
    def _create_fc_in_gdb(
        self,
        gdb_path: str,
        fc_name: str,
        geometry_type: str,
        spatial_reference: Optional[Union[int, arcpy.SpatialReference]] = None,
        feature_dataset: Optional[str] = None,
        template: Optional[str] = None,
        has_z: bool = False,
        has_m: bool = False,
        overwrite: bool = False,
    ) -> str:
        """
        Creates a feature class inside a file geodatabase.

        Returns the full path to the created feature class.
        """
        if not arcpy.Exists(gdb_path):
            raise ValueError(f"GDB does not exist: {gdb_path}")

        _GEOM_MAP = {
            "Point": "POINT",
            "Points": "POINT",
            "Polyline": "POLYLINE",
            "Line": "POLYLINE",
            "Lines": "POLYLINE",
            "Polygon": "POLYGON",
            "Polygons": "POLYGON",
        }
        
        geom = _GEOM_MAP.get(geometry_type)
        if not geom:
            raise ValueError(f"Unsupported geometry_type: {geometry_type}. Use point, line (polyline), or polygon.")

        out_path = os.path.join(gdb_path, feature_dataset) if feature_dataset else gdb_path
        out_fc = os.path.join(out_path, fc_name)

        if arcpy.Exists(out_fc):
            if overwrite:
                arcpy.management.Delete(out_fc)
            else:
                raise FileExistsError(f"Feature class already exists: {out_fc}")

        sr_obj = None
        if isinstance(spatial_reference, int):
            sr_obj = arcpy.SpatialReference(spatial_reference)
        elif isinstance(spatial_reference, arcpy.SpatialReference):
            sr_obj = spatial_reference

        arcpy.management.CreateFeatureclass(
            out_path=out_path,
            out_name=fc_name,
            geometry_type=geom,
            template=template,
            has_m="ENABLED" if has_m else "DISABLED",
            has_z="ENABLED" if has_z else "DISABLED",
            spatial_reference=sr_obj,
        )

        return out_fc

    def _has_z_m(self, fc_path: str) -> tuple[bool, bool]:
        d = arcpy.Describe(fc_path)
        return bool(d.hasZ), bool(d.hasM)   

    def _count_fc(self, fc_or_layer: str) -> int:
        try:
            return int(arcpy.management.GetCount(fc_or_layer)[0])
        except Exception:
            return 0

    def _copy_logs_and_feature_counts(self, parent_dir: str) -> tuple[int, int]:
        """
        Copies:
        - <parent_dir>/logs/log*.txt -> <parent_dir>/results/
        - <parent_dir>/feature_counts/clip_counts_grid_clip_*.csv -> <parent_dir>/results/

        Returns (num_logs_copied, num_csvs_copied)
        """
        base = Path(parent_dir)
        logs_dir = base / "logs"
        counts_dir = base / "feature_counts"
        results_dir = base / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        logs = list(logs_dir.glob("log*.txt")) if logs_dir.exists() else []
        csvs = list(counts_dir.glob("clip_counts_grid_clip_*.csv")) if counts_dir.exists() else []

        for src in logs:
            shutil.copy2(src, results_dir / src.name)

        for src in csvs:
            shutil.copy2(src, results_dir / src.name)

        return (len(logs), len(csvs))


    def _process_grid_sheet(self) -> Dict:
        """
        Processes each sheet in the Excel workbook and joins with gridzones from the geodatabase.
        Clips configured feature classes and stores outputs in a per sheet file geodatabase.
        - Skips annotation feature classes in the standard clip pipeline.
        - After the standard pipeline, finds all annotation feature classes in the source geodatabase
          and packages them as layer packages using the per sheet joined grid as the AOI.

        Returns:
            dict: {
                "success": bool,
                "data": <export_folder path or None>,
                "errors": [list of error strings]
            }
        """
        # Create new ClipAuditor
        clip_counter = ClipCounter(self.parent_dir, self.logger)
        clip_counter.open(run_label=_safe_run_label(f"grid_clip_{self.division_code or 'ALL'}"))

        errors = []
        warnings = []
        export_folder = os.path.join(self.parent_dir, "_export_temp")
        os.makedirs(export_folder, exist_ok=True)

        try:
            self.alternate_name_map = self._generate_alternate_name_map()
        except Exception as exc:
            error_msg = f"Failed to populate self.alternate_name_map with Error: {exc}"
            self.logger.error(error_msg)
            return {"success": False, "data": None, "errors": [error_msg]}
        try:
            workbook = openpyxl.load_workbook(self.gridzone_excel_path)
            sheet_names = workbook.sheetnames
            self.logger.info(f"Found Excel sheets: {sheet_names}")
        except Exception as e:
            error_msg = f"Failed to load Excel file '{self.gridzone_excel_path}': {e}"
            self.logger.error(error_msg)
            return {"success": False, "data": None, "errors": [error_msg]}

        try:
            for sheet_name in sheet_names:
                try:
                    safe_name = sheet_name.replace(" ", "_")
                    sheet_path = f"{self.gridzone_excel_path}/{sheet_name}$"

                    # Per sheet output gdb
                    per_sheet_gdb_name = f"{safe_name}_clipped.gdb"
                    per_sheet_gdb_path = os.path.join(export_folder, per_sheet_gdb_name)
                    if not arcpy.Exists(per_sheet_gdb_path):
                        arcpy.management.CreateFileGDB(export_folder, per_sheet_gdb_name)

                    self.logger.info(f"Processing sheet: {sheet_name} -> GDB: {per_sheet_gdb_name}")
                    arcpy.env.workspace = self.gdb_path
                    arcpy.env.overwriteOutput = True

                    joined_layer = arcpy.AddJoin_management(
                        in_layer_or_view=self._config["gridzones"]["feature_class_name_source"],
                        in_field=self._config["gridzones"]["GridZoneId_field"],
                        join_table=sheet_path,
                        join_field=self.join_excel_field_name,
                        join_type="KEEP_COMMON"
                    )[0]

                    output_grid = os.path.join(per_sheet_gdb_path, f"{safe_name}_gridzones")
                    arcpy.conversion.ExportFeatures(joined_layer, output_grid)
                    self.logger.info(f"Exported joined gridzones: {output_grid}")

                    # Gather annotation feature classes from:
                    # 1) Anything listed in feature_classes_to_clip that is annotation
                    # 2) Any annotation feature class found anywhere in the source gdb
                    annotation_fc_candidates = set()

                    clipped_outputs = {}
                    merge_tasks = []


                    fc_config_all = list(self.alternate_name_map["SourceDataName"])
                    
                    # -----------------------------------------------------------
                    # TODO - Not sure what to use this filter by division for?
                    # fc_config_filtered = self._filter_by_division(fc_config_all)
                    # -----------------------------------------------------------

                    # Default merge is not used
                    merge_with = None
                    output_clip_fc_name = ""

                    # for i, source_data_name in enumerate(fc_config_filtered):
                    for i, source_data_name in enumerate(fc_config_all):
                        # Continuing with non-"MERGE_LAYERS" SourceDataNames in config
                        # Confirm getting the excel-based alternate name for feature class
                        pre_clip_attribute_query = self.alternate_name_map["PreClipAttributeQuery"][i]
                        intermediate_clip_filter_name = self.alternate_name_map["IntermediateClipFilterName"][i]                    
                        post_clip_attribute_query = self.alternate_name_map["PostClipAttributeQuery"][i]
                        intermediate_post_clip_filter_name = self.alternate_name_map["IntermediatePostClipFilterName"][i]
                        intermediate_merge_clip_name = self.alternate_name_map["IntermediateMergeClipName"][i]
                        final_output_name = self.alternate_name_map["OutputName"][i]
                        
                        is_anno_layer = self.alternate_name_map["IsAnnotationLayer"][i]
                        include_in_final_result = self.alternate_name_map["IncludeInFinalResult"][i]

                        # Section for handling merging of layers that were already clipped
                        if source_data_name == 'MERGE_LAYERS' \
                            and pre_clip_attribute_query == 'NONE' \
                            and post_clip_attribute_query == 'NONE' \
                            and intermediate_post_clip_filter_name == 'NONE' \
                            and intermediate_merge_clip_name != 'NONE' \
                            and "," in intermediate_clip_filter_name: 

                            # Track Merge Specs only if a merge result is expected
                            merge_with = intermediate_clip_filter_name
                            output_clip_fc_name = f"{intermediate_merge_clip_name}"
                            output_clip_fc_path = os.path.join(per_sheet_gdb_path, output_clip_fc_name)
                        
                        # For non-merging results
                        else:
                            output_clip_fc_name = f"{intermediate_clip_filter_name}"
                            output_clip_fc_path = os.path.join(per_sheet_gdb_path, output_clip_fc_name)

                        try:
                            # Run through a check that the source_data_name is in the file gdb
                            if source_data_name != 'MERGE_LAYERS' and not self._existsInFileGdb(self.gdb_path, source_data_name):
                                msg = f"Could not clip {source_data_name} because it does not exist in the file geodatabase: {self.gdb_path}. Continuing with execution..."
                                self.logger.warning(msg)
                                warnings.append(msg)
                                continue # skip processing this feature class because it doesn't exist

                            # Pre-Clip Attribute Query & Then Clip
                            if source_data_name != 'MERGE_LAYERS' and self._existsInFileGdb(self.gdb_path, source_data_name):
                                if pre_clip_attribute_query != 'NONE':
                                    lyr = f"{intermediate_clip_filter_name}_lyr"
                                    arcpy.MakeFeatureLayer_management(source_data_name, lyr)
                                    arcpy.SelectLayerByAttribute_management(lyr, "NEW_SELECTION", pre_clip_attribute_query)
                                    arcpy.analysis.Clip(lyr, output_grid, output_clip_fc_path)
                                    arcpy.Delete_management(lyr)
                                else:
                                    arcpy.analysis.Clip(source_data_name, output_grid, output_clip_fc_path)

                                self.logger.info(f"Clipped {source_data_name} to {output_clip_fc_path}")
                                fc_path = os.path.join(self.gdb_path, source_data_name)

                                source_count = self._count_fc(fc_path)
                                clipped_count = self._count_fc(output_clip_fc_path)
                                selected_count = source_count 

                                clip_counter.add_row(
                                    sheet=sheet_name,
                                    source_name=source_data_name,
                                    output_name=final_output_name,
                                    source_count=source_count,
                                    selected_count=selected_count,
                                    clipped_count=clipped_count,
                                    note=f"pre={pre_clip_attribute_query}" if pre_clip_attribute_query != "NONE" else ""
                                )

                                clipped_outputs[intermediate_clip_filter_name] = {"path": output_clip_fc_path, "was_merged": False, "final_name": final_output_name}

                                # Run Post-Clip Attribute Query
                                if post_clip_attribute_query != 'NONE':
                                    # Modify post clip attribute query to run sub-query

                                    output_post_clip_fc_name = f"{intermediate_post_clip_filter_name}"
                                    output_post_clip_fc_path = os.path.join(per_sheet_gdb_path, output_post_clip_fc_name)
                                    lyr = f"{intermediate_post_clip_filter_name}_lyr"

                                    full_post_clip_query = self._process_post_clip_attribute_query(per_sheet_gdb_path, post_clip_attribute_query)
                                    
                                    # Use the post-clipped layer to select the data on
                                    arcpy.MakeFeatureLayer_management(output_post_clip_fc_name, lyr)
                                    arcpy.SelectLayerByAttribute_management(lyr, "NEW_SELECTION", full_post_clip_query)
                                    arcpy.analysis.Clip(lyr, output_grid, output_post_clip_fc_path)
                                    arcpy.Delete_management(lyr)

                            # For only those 'MERGE_LAYERS' rows
                            elif source_data_name == 'MERGE_LAYERS' and not self._existsInFileGdb(self.gdb_path, final_output_name):
                                if merge_with:

                                    first_feature_class_to_merge_with = intermediate_clip_filter_name.split(",")[0]
                                    other_feature_classes_to_merge_with = intermediate_clip_filter_name.split(",")[1:]
                                    if first_feature_class_to_merge_with != 'NONE':
                                        merge_tasks.append({"final_name": final_output_name, "base": first_feature_class_to_merge_with, "merge_with": other_feature_classes_to_merge_with})

                                    # Get source feature rows count for auditing
                                    source_count = self._count_fc(source_data_name)
                                    selected_count = source_count
                                    clipped_count = 0  # No clipping occurs for MERGE_LAYERS rows

                                    clip_counter.add_row(
                                        sheet=sheet_name,
                                        source_name=source_data_name,
                                        output_name=final_output_name,
                                        source_count=source_count,
                                        selected_count=selected_count,
                                        clipped_count=clipped_count,
                                        note=f"pre={pre_clip_attribute_query}" if pre_clip_attribute_query != "NONE" else ""
                                    )

                                clipped_outputs[intermediate_clip_filter_name] = {"path": output_clip_fc_path, "was_merged": False, "final_name": final_output_name}

                            else:
                                msg = f"Skipping clip for {source_data_name}; another problem occurred. Does this feature class exist?"
                                self.logger.warning(msg)
                                errors.append(msg)
                                continue
                            
                            if is_anno_layer.lower() == 'yes' and include_in_final_result.lower() == 'yes':
                                # annotation_details = self._is_annotation_fc(output_clip_fc_path, is_anno_layer)
                                # if annotation_details["feature_class_is_annotation"] or annotation_details["is_configured_as_annotation"].lower() == 'yes':
                                annotation_fc_candidates.add(final_output_name)       

                        except Exception as clip_err:
                            msg = f"Could not clip {source_data_name}: {clip_err}"
                            self.logger.error(msg)
                            errors.append(msg)

                    # Merging Tasks
                    for task in merge_tasks:
                        final_fc_name = task["final_name"]
                        base_name = task["base"]
                        merge_names = task["merge_with"]

                        base_fc_entry = clipped_outputs.get(base_name)
                        if not base_fc_entry:
                            msg = f"Skipping merge for {base_name}; base not clipped."
                            self.logger.warning(msg)
                            errors.append(msg)
                            continue
                        
                        
                        base_fc = base_fc_entry["path"]
                        merge_inputs = [base_fc]

                        for merge_name in merge_names:
                            merge_entry = clipped_outputs.get(merge_name)
                            if merge_entry and arcpy.Exists(merge_entry["path"]):
                                merge_inputs.append(merge_entry["path"])
                                clipped_outputs[merge_name]["was_merged"] = True
                            else:
                                msg = f"Layer to merge not found or not clipped: {merge_name}"
                                self.logger.warning(msg)
                                errors.append(msg)

                        if len(merge_inputs) > 1:
                            try:
                                merged_output_fc = os.path.join(per_sheet_gdb_path, f"{final_fc_name}")
                                arcpy.management.Merge(merge_inputs, merged_output_fc)
                                self.logger.info(f"Merged {merge_inputs} into {merged_output_fc}")

                                merged_count = self._count_fc(merged_output_fc)

                                clip_counter.add_row(
                                    sheet=sheet_name,
                                    source_name="MERGE",
                                    output_name=final_fc_name,
                                    source_count=sum(self._count_fc(p) for p in merge_inputs),
                                    selected_count=sum(self._count_fc(p) for p in merge_inputs),
                                    clipped_count=0,
                                    merged_count=merged_count,
                                    note=" + ".join([os.path.basename(p) for p in merge_inputs])
                                )
                                
                                # Add main layer to clipped_outputs
                                clipped_outputs[final_fc_name] = {"path": merged_output_fc, "was_merged": True}
                            except Exception as merge_err:
                                msg = f"Merge failed for {final_fc_name}: {merge_err}"
                                self.logger.warning(msg)
                                errors.append(msg)

                    for output_name, info in clipped_outputs.items():
                        if not info["was_merged"]:
                            try:
                                final_name = info["final_name"]

                                # Skip 'NONE'
                                if final_name == 'NONE': 
                                    continue

                                output_shapefile = os.path.join(export_folder, f"{final_name}.shp")
                                arcpy.conversion.FeatureClassToFeatureClass(
                                    in_features=info["path"],
                                    out_path=export_folder,
                                    out_name=f"{final_name}.shp"
                                )
                                self.logger.info(f"Exported unmerged clipped result {output_name} to shapefile: {output_shapefile}")
                            except Exception as shp_err:
                                msg = f"Failed to export {info['path']} to shapefile: {shp_err} [in code: `for output_name, info in clipped_outputs.items():`]"
                                self.logger.warning(msg)
                                errors.append(msg)
                                
                    # Export unmerged standard clipped results to shapefile
                    for output_name, info in clipped_outputs.items():

                        # Export shp as final name now
                        
                        if info["was_merged"]:
                            try:
                                if 'final_name' in info:
                                    final_name = info["final_name"]
                                else:
                                    final_name = output_name
                                out_name = f"{final_name}.shp"
                                
                                # Skip 'NONE'
                                if final_name == 'NONE': 
                                    continue

                                arcpy.conversion.FeatureClassToFeatureClass(
                                    in_features=info["path"],
                                    out_path=export_folder,
                                    out_name=out_name
                                )
                                self.logger.info(f"Exported unmerged clipped result to shapefile: {os.path.join(export_folder, out_name)}")
                            except Exception as shp_err:
                                msg = f"Failed to export {info['path']} to shapefile: {shp_err}"
                                self.logger.warning(msg)
                                errors.append(msg)

                    # After standard pipeline, handle annotation feature classes with packaging
                    # Use the per sheet joined grid (output_grid) as the AOI polygon
                    for ann_fc in sorted(annotation_fc_candidates):
                        try:
                            layer_name = f"{os.path.basename(ann_fc)}"
                            self.clip_annotation_to_polygon_and_package(
                                gdb_path=per_sheet_gdb_path,
                                annotation_fc=os.path.join(export_folder, ann_fc + ".shp"),
                                polygon_fc=output_grid,
                                polygon_where="", # TODO: Future option to allow user to select which annotations to select
                                layer_name=layer_name
                            )
                            self.logger.info(f"Packaged annotation to LPKX for: {ann_fc}")
                        except Exception as ann_err:
                            msg = f"Annotation packaging failed for {ann_fc}: {ann_err}"
                            self.logger.warning(msg)
                            errors.append(msg)

                    self.logger.info(f"Completed sheet: {sheet_name}")

                except Exception as sheet_err:
                    msg = f"Error processing sheet '{sheet_name}': {sheet_err}"
                    self.logger.error(msg)
                    errors.append(msg)

            self.logger.info(f"All sheets processed. Outputs stored in: {export_folder}")


            return {
                "success": len(errors) == 0,
                "data": export_folder if len(errors) == 0 else None,
                "errors": errors,
                "warnings": warnings
            }
    
        finally:
            try:
                # Finish writing the feature counts to the csv record
                clip_counter.write()

                n_logs, n_csvs = self._copy_logs_and_feature_counts(self.parent_dir)
                self.logger.info(f"Copied {n_logs} log files and {n_csvs} feature count CSV files into {Path(self.parent_dir)/'results'}")         
            except Exception as e:
                self.logger.warning(f"ClipAudit.write failed: {e}")


    def clip_annotation_to_polygon_and_package(
        self, 
        gdb_path: str,
        annotation_fc: str,            # e.g. r"C:\path\to\data.gdb\Anno_StreetNames"
        polygon_fc: str,               # e.g. r"C:\path\to\data.gdb\AOI_Polygons"
        polygon_where: str="",         # optional SQL to pick subset of polygons, e.g. "NAME = 'District 7'"
        layer_name: str="Clipped_Annotation",
    ):
        """
        Selects annotation features that intersect a polygon area of interest and packages them as .lpkx.

        Parameters
        ----------
        gdb_path : str -  Path to a file geodatabase to use for intermediate outputs.
        annotation_fc : str - Full path to the annotation feature class to subset.
        polygon_fc : str = Full path to polygon feature class that defines the clipping AOI.
        polygon_where : str, optional - An optional SQL where clause to restrict AOI polygons.
        output_lpkx : str - Output .lpkx path.
        layer_name : str - Friendly name for the output layer inside the package.
        """
        arcpy.env.overwriteOutput = True

        # Validate inputs
        for p in [annotation_fc, polygon_fc]:
            if not arcpy.Exists(p):
                raise FileNotFoundError(f"Input does not exist: {p}")

        if not arcpy.Exists(gdb_path):
            raise FileNotFoundError(f"Geodatabase does not exist: {gdb_path}")

        # Create a private scratch workspace inside the provided gdb_path folder
        workspace_dir = os.path.dirname(gdb_path)
        tmp_dir = tempfile.mkdtemp(prefix="anno_clip_", dir=workspace_dir)
        tmp_gdb = os.path.join(tmp_dir, "scratch.gdb")
        output_lpkx = os.path.join(workspace_dir, f"{layer_name.replace(' ', '_')}.lpkx")
        arcpy.management.CreateFileGDB(tmp_dir, "scratch.gdb")

        errors = []

        try:
            # 1) Build a single AOI polygon by optional selection then dissolve
            aoi_layer = "aoi_layer"
            arcpy.management.MakeFeatureLayer(polygon_fc, aoi_layer, polygon_where if polygon_where else None)

            aoi_selected = os.path.join(tmp_gdb, "AOI_Selected")
            arcpy.management.CopyFeatures(aoi_layer, aoi_selected)

            aoi_single = os.path.join(tmp_gdb, "AOI_Dissolved")
            arcpy.management.Dissolve(aoi_selected, aoi_single)  # no fields, creates a single-part multipolygon if needed

            # 2) Select annotation that intersects the AOI
            anno_layer = "anno_layer"
            arcpy.management.MakeFeatureLayer(annotation_fc, anno_layer)

            arcpy.management.SelectLayerByLocation(
                in_layer=anno_layer,
                overlap_type="INTERSECT",
                select_features=aoi_single,
                selection_type="NEW_SELECTION"
            )

            # 3) Copy the selection to a new annotation feature class
            anno_subset = os.path.join(tmp_gdb, layer_name)
            arcpy.management.CopyFeatures(anno_layer, anno_subset)

            # 4) Create a layer file (.lyrx) from the subset
            lyrx_path = os.path.join(tmp_dir, f"{layer_name}.lyrx")
            subset_layer = "subset_layer"
            arcpy.management.MakeFeatureLayer(anno_subset, subset_layer, None)
            
            # Give it a nice name
            # BAD CALL
            # arcpy.AlterAliasName(subset_layer, layer_name)
            
            arcpy.management.SaveToLayerFile(subset_layer, lyrx_path, "RELATIVE")

            # 5) Package to .lpkx
            # PackageLayer in Pro creates .lpkx, which is the modern layer package format
            arcpy.management.PackageLayer(
                in_layer=lyrx_path,
                output_file=output_lpkx,
                convert_data="CONVERT",
                summary=f"Subset of {os.path.basename(annotation_fc)} intersecting AOI",
                tags="annotation, subset, packaging",
                select_related_rows="KEEP_ONLY_RELATED_ROWS"
            )

            print(f"Created layer package: {output_lpkx}")

        except Exception as e:
            msg = f"Error during clipping annotation layers: {e}"
            self.logger.error(msg)
            errors.append(msg)
            return {"success": False, "data": None, "errors": errors}
        finally:
            # Clean up temp artifacts but keep the output package
            try:
                # Clean locks
                helpers.clear_locks()
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass


    def _export_all_feature_classes_to_shapefiles(
            self,
            gdb_path: str,
            out_folder: Optional[str] = None,
            include_feature_datasets: bool = True,
            prefix_with_dataset: bool = True,
            overwrite: bool = True
        ) -> List[str]:
        """
        Export all feature classes in a File Geodatabase to shapefiles in the parent folder.

        Params
        - gdb_path: path to a .gdb folder on disk.
        - out_folder: output folder. If None, uses the parent directory of the .gdb.
        - include_feature_datasets: if True, recurse into all feature datasets.
        - prefix_with_dataset: if True, prepend the feature dataset name to the shapefile name.
        - overwrite: if True, overwrite existing shapefiles.

        Returns
        - List of full paths to the created shapefiles.
        """
        gdb = Path(gdb_path)
        if not gdb.exists() or gdb.suffix.lower() != ".gdb" or not gdb.is_dir():
            raise ValueError(f"Invalid File Geodatabase path: {gdb_path}")

        out_dir = Path(out_folder) if out_folder else gdb.parent
        out_dir.mkdir(parents=True, exist_ok=True)

        arcpy.env.overwriteOutput = overwrite

        created: List[str] = []
        used_names = set()

        # Collect feature classes
        fc_items = self._list_feature_classes(str(gdb), include_feature_datasets, prefix_with_dataset)

        # Export each feature class
        for fc_path, suggested_name in fc_items:
            shp_name = self._check_unique_name(suggested_name, used_names)
            out_shp = str(out_dir / f"{shp_name}.shp")

            # Use CopyFeatures so we can control the output name precisely
            arcpy.management.CopyFeatures(fc_path, out_shp)
            created.append(out_shp)
            print(f"Exported: {fc_path} -> {out_shp}")

        return created

    
    def _check_unique_name(self, base: str, used_names: Set[str]) -> str:
        """
        Ensure the shapefile base name is unique within this run.
        Appends _1, _2, ... if needed, while keeping within 13 char limit.
        """
        candidate = self._sanitize_for_shapefile(base)
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        # Try suffixes
        for i in range(1, 1000):
            suffix = f"_{i}"
            max_base_len = 13 - len(suffix)
            trimmed = candidate[:max_base_len]
            cand2 = f"{trimmed}{suffix}"
            if cand2 not in used_names:
                used_names.add(cand2)
                return cand2
        raise RuntimeError("Could not create a unique shapefile name.")
        
    def _list_feature_classes(
            self, 
            workspace: str, 
            include_feature_datasets: bool, 
            prefix_with_dataset: bool
        ) -> List[Tuple[str, str]]:
        """
        Returns list of tuples (full_fc_path, suggested_name)
        - suggested_name may include dataset prefix if configured
        """
        arcpy.env.workspace = workspace
        results: List[Tuple[str, str]] = []

        # Top level feature classes
        for fc in arcpy.ListFeatureClasses() or []:
            full = os.path.join(workspace, fc)
            base_name = Path(fc).name
            results.append((full, base_name))

        if include_feature_datasets:
            for ds in arcpy.ListDatasets("", "Feature") or []:
                ds_path = os.path.join(workspace, ds)
                arcpy.env.workspace = ds_path
                for fc in arcpy.ListFeatureClasses() or []:
                    full = os.path.join(ds_path, fc)
                    base_name = f"{ds}_{fc}" if prefix_with_dataset else fc
                    results.append((full, base_name))

        return results

    def _sanitize_for_shapefile(self, name: str) -> str:
        """
        Shapefile base name rules:
        - Max 13 chars
        - Letters, numbers, underscore only
        - Cannot start with a number in older tools, so we prefix with '_' if needed
        """
        # Replace invalid chars with underscore
        cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", name)
        # Trim to 13 characters
        cleaned = cleaned[:13]
        # Ensure not empty
        if not cleaned:
            cleaned = "layer"
        # Ensure does not start with a digit
        if cleaned[0].isdigit():
            cleaned = f"_{cleaned[:-1]}" if len(cleaned) == 13 else f"_{cleaned}"
        return cleaned

    def export_feature_collections(
            self,
            input_folder: Optional[str] = None, 
            output_folder: Optional[str] = None
        ) -> Dict[str, Union[bool, str, List[str], None]]:
        """
        Invokes RecursiveExportFeatureCollection to export JSON FeatureCollections and create a mobile geodatabase.

        Returns:
            dict: {
                "success": bool,
                "data": <output_folder path or None>,
                "errors": [list of error strings]
            }
        """
        errors = []
        warnings = []
        in_dir = self.parent_dir
        out_dir = os.path.join(self.parent_dir, "results")
        os.makedirs(out_dir, exist_ok=True)

        tool = RecursiveExportFeatureCollection()

        class MockParam:
            def __init__(self, val): self.valueAsText = val

        params = [MockParam(in_dir), MockParam(out_dir)]

        try:
            tool.execute(params, logger_=self.logger)
            self.logger.info(f"Recursive export completed: {out_dir}")
            return {"success": True, "data": out_dir, "errors": []}
        except Exception as e:
            msg = f"Error during recursive export: {e}"
            self.logger.error(msg)
            errors.append(msg)
            return {"success": False, "data": None, "errors": errors, "warnings": warnings}


# For Testing or direct script run
def run_all(self) -> Dict[str, Union[bool, str, List[str], None]]:
    result = self._process_grid_sheet()
    if not result["success"]:
        return result

    export_root = os.path.join(self.parent_dir, "_export_temp")
    fc_out = os.path.join(self.parent_dir, "feature_collections")
    os.makedirs(fc_out, exist_ok=True)

    return self.export_feature_collections(input_folder=export_root, output_folder=fc_out)