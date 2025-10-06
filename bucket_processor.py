from buckets import BucketsBase, BucketsVRH
import os
import logging
import pandas as pd
import geopandas as gpd
import uuid

logger = logging.getLogger(__name__)

# -----------------------
# Helper functions
# -----------------------

def only_match_id(df: pd.DataFrame, gisib_id_col: str, bgt_id_col: str) -> pd.DataFrame:
    """Return only the two ID columns."""
    return df[[gisib_id_col, bgt_id_col]].copy()


def match_id_and_remove(df: pd.DataFrame, gisib_id_col: str, bgt_id_col: str):
    """
    Keep a single (largest area) row per BGT id; return the matches to keep and
    a DataFrame of GISIB ids to remove (duplicates by BGT id).
    """
    df_result = (df
        .copy()
        .assign(area=lambda d: d.geometry.area)
        .sort_values(by="area", ascending=False)
    )
    df_remove = (
          df_result.loc[lambda d: d.duplicated(subset=[bgt_id_col]), [gisib_id_col]]
    )

    df_change_geometry = (
        df_result
        .loc[lambda d: ~d.duplicated(subset=[bgt_id_col])]
        .loc[:,[gisib_id_col, bgt_id_col, "geometry_bgt"]]
        .set_geometry("geometry_bgt")
        .rename_geometry("geometry")
        .set_crs("EPSG:28992")
    )

    mask = ~df_result[gisib_id_col].isin(df_remove[gisib_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, df_change_geometry,df_remove


def match_id_and_add(df: pd.DataFrame, gisib_id_col: str, bgt_id_col: str, gisib_df: pd.DataFrame):
    """
    Identify GISIB ids that need to be split (duplicates by GISIB id) and build
    new objects using BGT geometry. Returns:
      - result_df: the matches to accept (excluding those that will be 'added')
      - add_objects: GeoDataFrame of new features to add (EPSG:28992)
    """
    if "area" not in df.columns:
        df = df.assign(area=df.geometry.area)

    df_result = df[[gisib_id_col, bgt_id_col]].copy()

    df_objects = (
        df
        .assign(area_bgt = lambda df: df.geometry_bgt.area)
        .sort_values(by="area_bgt",ascending=False)
          .assign(KEEP_GUID = ~df.duplicated(subset=[gisib_id_col],keep="first"))
          .loc[:,[gisib_id_col, bgt_id_col, "geometry_bgt","KEEP_GUID"]]
    )

    df_add = (
        df_objects
        .loc[lambda df: df.KEEP_GUID == False]
    )

    # Build GeoDataFrame of additions: take attributes from GISIB object, geometry from BGT
    add_objects = (
        gisib_df.drop(columns=["geometry"], errors="ignore")
                .merge(df_add, how="inner", on=gisib_id_col)
    )

    # Ensure geometry column is set from geometry_bgt and CRS is EPSG:28992 (Amersfoort)
    add_objects = gpd.GeoDataFrame(add_objects, geometry="geometry_bgt")
    add_objects = add_objects.rename_geometry("geometry")
    add_objects = add_objects.set_crs("EPSG:28992")

    # Remove unwanted columns if present
    columns_to_remove = {
        "IDENTIFICATIE", "IMGEOID", "VRH_ID", "GRN_ID", "TRD_ID", "ID","KEEP_GUID"
    }
    cols_to_drop = [col for col in columns_to_remove if col in add_objects.columns]
    if cols_to_drop:
        add_objects = add_objects.drop(columns=cols_to_drop)


    new_guid_values = [
        "{" + str(uuid.uuid4()).upper() + "}" for _ in range(len(add_objects))
    ]



    change_geometry_objects = (
                                  df_objects
                                  .loc[lambda df: df.KEEP_GUID == True,
                                  [gisib_id_col,bgt_id_col,"geometry_bgt"]]
                                  .set_geometry("geometry_bgt")
                                  .rename_geometry("geometry")
                                  .set_crs("EPSG:28992")
                                  .assign(change="change")
    )

    assert len(new_guid_values) ==len(add_objects)
    add_objects = add_objects.assign(GUID = new_guid_values)

    # add_objects = add_objects.drop(columns=["DUP_GUID"])

    # result_df excludes the rows that will be added (those in df_add by BGT id)
    mask = ~df_result[bgt_id_col].isin(df_add[bgt_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, change_geometry_objects, add_objects


# -----------------------
# Bucket configuration
# -----------------------

BUCKET_MATCH_CONFIG = {
    BucketsBase.BUCKET1.value: {"function": only_match_id,      "mode": "only"},   # geom_1_to_1
    BucketsBase.BUCKET2.value: {"function": match_id_and_remove,"mode": "remove"}, # gisib_merge
    BucketsBase.BUCKET4.value: {"function": match_id_and_add,   "mode": "add"},    # gisib_split
    BucketsBase.BUCKET5.value: {"function": only_match_id,      "mode": "only"},   # geom_75_match
    BucketsVRH.BUCKET6.value:  {"function": only_match_id,      "mode": "only"},   # geom_overlap_150_match (VRH)
}


# -----------------------
# Orchestration: export per asset
# -----------------------
def process_and_export_per_asset_mode(
    filtered_auto_buckets: dict[str, dict],
    gisib_datasets: dict[str, pd.DataFrame],
    gisib_id_col: str,
    bgt_id_col: str,
    output_dir: str
):
    """
    For each asset:
      - Excel matched_<asset>.xlsx:
          * 'match' sheet with accepted pairs
          * 'remove' sheet with GISIB ids to remove
      - GeoPackage adds_<asset>.gpkg:
          * layer='add'               -> additions (EPSG:28992)
          * layer='change_geometry'   -> geometry updates (EPSG:28992)

    Returns:
      all_additions:        dict[asset] -> GeoDataFrame of adds
      all_removals:         dict[asset] -> DataFrame of GISIB ids to remove
      all_geometry_changes: dict[asset] -> GeoDataFrame of geometry updates (from add/remove)
    """
    os.makedirs(output_dir, exist_ok=True)
    all_additions = {}
    all_removals = {}
    all_geometry_changes = {}

    for asset, bucket_dict in filtered_auto_buckets.items():
        print(asset)
        logger.info(f"Processing asset: {asset}")
        match_rows = []
        add_rows = []
        remove_rows = []
        change_geom_rows = []  # NEW: collect change-geometry from both branches

        for bucket_enum, df in bucket_dict.items():
            print(bucket_enum)
            print(df.shape)
            bucket_label = bucket_enum  # assuming enum.value already used as key
            config = BUCKET_MATCH_CONFIG.get(bucket_label)

            if config is None:
                logger.warning(f"No match config for bucket '{bucket_label}' on asset '{asset}', skipping.")
                continue

            match_function = config["function"]
            mode = config["mode"]
            logger.info(f"Applying mode '{mode}' with bucket '{bucket_label}' on asset '{asset}'")

            if mode == "add":
                # returns: result_df, change_geometry_df, add_df
                result_df, change_geometry_df, add_df = match_function(df, gisib_id_col, bgt_id_col, gisib_datasets[asset])
                if add_df is not None and not add_df.empty:
                    add_rows.append(add_df)
                if change_geometry_df is not None and not change_geometry_df.empty:
                    change_geom_rows.append(change_geometry_df)
                print("add")
                print(len(add_df) if add_df is not None else 0)
                print(result_df.shape if result_df is not None else (0, 0))

            elif mode == "remove":
                # returns: result_df, change_geometry_df, remove_df
                result_df, change_geometry_df, remove_df = match_function(df, gisib_id_col, bgt_id_col)
                if remove_df is not None and not remove_df.empty:
                    remove_rows.append(remove_df[[gisib_id_col]])
                if change_geometry_df is not None and not change_geometry_df.empty:
                    change_geom_rows.append(change_geometry_df)
                print("remove")
                print(len(remove_df) if remove_df is not None else 0)
                print(result_df.shape if result_df is not None else (0, 0))

            elif mode == "only":
                print("only")
                result_df = match_function(df, gisib_id_col, bgt_id_col)
                print(result_df.shape if result_df is not None else (0, 0))

            else:
                logger.error(f"Unsupported mode '{mode}' for bucket '{bucket_label}'")
                continue

            if result_df is not None and not result_df.empty:
                match_rows.append(result_df[[gisib_id_col, bgt_id_col]])

        # ---- Excel (match + remove) ----
        asset_excel_path = os.path.join(output_dir, f"matched_{asset}.xlsx")
        with pd.ExcelWriter(asset_excel_path) as writer:
            if match_rows:
                pd.concat(match_rows, ignore_index=True).to_excel(writer, sheet_name="match", index=False)
            if remove_rows:
                combined_removes = pd.concat(remove_rows, ignore_index=True).drop_duplicates()
                combined_removes.to_excel(writer, sheet_name="remove", index=False)
                all_removals[asset] = combined_removes
        logger.info(f"✅ Exported match/remove for '{asset}' to {asset_excel_path}")

        # ---- GeoPackage (add + change_geometry) ----
        gpkg_path = os.path.join(output_dir, f"adds_{asset}.gpkg")

        # Helper: coerce to GeoDataFrame with geometry from 'geometry' or fallback 'geometry_bgt'
        def _to_crs_28992(gdf_like):
            if isinstance(gdf_like, gpd.GeoDataFrame):
                gdf = gdf_like
                if gdf.crs is None or str(gdf.crs).upper() != "EPSG:28992":
                    gdf = gdf.set_crs("EPSG:28992", allow_override=True)
                return gdf
            # DataFrame: pick geometry column
            geom_col = "geometry" if "geometry" in gdf_like.columns else "geometry_bgt"
            gdf = gpd.GeoDataFrame(gdf_like.copy(), geometry=geom_col)
            # rename to standard 'geometry' if needed
            if geom_col != "geometry":
                gdf = gdf.rename_geometry("geometry")
            return gdf.set_crs("EPSG:28992")

        have_adds = bool(add_rows)
        have_changes = bool(change_geom_rows)
        if have_adds or have_changes:
            if os.path.exists(gpkg_path):
                os.remove(gpkg_path)  # start clean so both layers are written fresh

        # Additions layer
        if have_adds:
            combined_adds = pd.concat(add_rows, ignore_index=True)
            combined_adds = _to_crs_28992(combined_adds)
            print(type(combined_adds))
            print(combined_adds.columns)
            print(combined_adds.info())
            print(combined_adds)
            combined_adds.to_file(gpkg_path, layer="add", driver="GPKG")
            all_additions[asset] = combined_adds
            logger.info(f"🗺️  Exported 'add' layer for '{asset}' to {gpkg_path}")

        # Change-geometry layer (from add + remove)
        if have_changes:
            combined_changes = pd.concat(change_geom_rows, ignore_index=True)
            combined_changes = _to_crs_28992(combined_changes)
            # keep useful ID columns if present
            keep_first = [c for c in [gisib_id_col, bgt_id_col] if c in combined_changes.columns]
            other_cols = [c for c in combined_changes.columns if c not in keep_first + ["geometry"]]
            combined_changes = combined_changes[keep_first + other_cols + ["geometry"]]
            combined_changes.to_file(gpkg_path, layer="change_geometry", driver="GPKG")
            all_geometry_changes[asset] = combined_changes
            logger.info(f"🗺️  Exported 'change_geometry' layer for '{asset}' to {gpkg_path}")

    return all_additions, all_removals, all_geometry_changes
