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
    df_result = df[[gisib_id_col, bgt_id_col]].copy()

    df_remove = (
        df.assign(area=lambda d: d.geometry.area)
          .sort_values(by="area", ascending=False)
          .loc[lambda d: d.duplicated(subset=[bgt_id_col]), [gisib_id_col]]
    )

    mask = ~df_result[gisib_id_col].isin(df_remove[gisib_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, df_remove


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

    df_add = (
        df
        .assign(area_bgt = lambda df: df.geometry_bgt.area)
        .sort_values(by="area_bgt",ascending=False)
          .assign(DUP_GUID = ~df.duplicated(subset=[gisib_id_col],keep="last"))
          .loc[:,[gisib_id_col, bgt_id_col, "geometry_bgt","DUP_GUID"]]
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
        "IDENTIFICATIE", "IMGEOID", "VRH_ID", "GRN_ID", "TRD_ID", "ID"
    }
    cols_to_drop = [col for col in columns_to_remove if col in add_objects.columns]
    if cols_to_drop:
        add_objects = add_objects.drop(columns=cols_to_drop)



    mask = add_objects.DUP_GUID == True

    new_guid_values = [
        "{" + str(uuid.uuid4()).upper() + "}" for _ in range(mask.sum())
    ]

    assert len(new_guid_values) ==sum(mask)
    add_objects.loc[mask,"GUID"] = new_guid_values

    # add_objects = add_objects.drop(columns=["DUP_GUID"])

    # result_df excludes the rows that will be added (those in df_add by BGT id)
    mask = ~df_result[bgt_id_col].isin(df_add[bgt_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, add_objects


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
      - Write 'match' and 'remove' to an Excel file matched_<asset>.xlsx (no 'add' sheet).
      - Write 'add' to a GeoPackage adds_<asset>.gpkg (layer='add', EPSG:28992).
    Returns:
      all_additions: dict[asset] -> GeoDataFrame of adds
      all_removals:  dict[asset] -> DataFrame of GISIB ids to remove
    """
    os.makedirs(output_dir, exist_ok=True)
    all_additions = {}
    all_removals = {}

    for asset, bucket_dict in filtered_auto_buckets.items():
        print(asset)
        logger.info(f"Processing asset: {asset}")
        match_rows = []
        add_rows = []
        remove_rows = []

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
                result_df, add_df = match_function(df, gisib_id_col, bgt_id_col, gisib_datasets[asset])
                add_rows.append(add_df)
                print("add")
                print(len(add_df))
                print(result_df.shape)
            elif mode == "remove":
                result_df, remove_df = match_function(df, gisib_id_col, bgt_id_col)
                if not remove_df.empty:
                    remove_rows.append(remove_df[[gisib_id_col]])
                    print("remove")
                    print(len(remove_df))
                    print(result_df.shape)
            elif mode == "only":
                print("only")
                result_df = match_function(df, gisib_id_col, bgt_id_col)
                print(result_df.shape)
            else:
                logger.error(f"Unsupported mode '{mode}' for bucket '{bucket_label}'")
                continue

            match_rows.append(result_df[[gisib_id_col, bgt_id_col]])

        # ---- Excel (match + remove) ----
        asset_excel_path = os.path.join(output_dir, f"matched_{asset}.xlsx")
        with pd.ExcelWriter(asset_excel_path) as writer:
            if match_rows:
                pd.concat(match_rows, ignore_index=True).to_excel(writer, sheet_name="match", index=False)
            if remove_rows:
                combined_removes = pd.concat(remove_rows, ignore_index=True)
                combined_removes.to_excel(writer, sheet_name="remove", index=False)
                all_removals[asset] = combined_removes

        logger.info(f"✅ Exported match/remove for '{asset}' to {asset_excel_path}")

        # ---- GeoPackage (add layer) ----
        if add_rows:
            combined_adds = pd.concat(add_rows, ignore_index=True)

            # Make sure it's a proper GeoDataFrame and CRS is EPSG:28992
            if not isinstance(combined_adds, gpd.GeoDataFrame):
                combined_adds = gpd.GeoDataFrame(combined_adds, geometry="geometry", crs="EPSG:28992")
            else:
                # Ensure CRS set (and correct) even if upstream already set it
                if combined_adds.crs is None or str(combined_adds.crs).upper() != "EPSG:28992":
                    combined_adds = combined_adds.set_crs("EPSG:28992", allow_override=True)

            gpkg_path = os.path.join(output_dir, f"adds_{asset}.gpkg")
            # Overwrite the file if it exists to avoid layer append confusion
            if os.path.exists(gpkg_path):
                os.remove(gpkg_path)

            print(type(combined_adds))
            print(combined_adds.columns)
            print(combined_adds.info())
            print(combined_adds)
            combined_adds.to_file(gpkg_path, layer="add", driver="GPKG")

            all_additions[asset] = combined_adds
            logger.info(f"🗺️  Exported add layer for '{asset}' to {gpkg_path} (layer: 'add')")

    return all_additions, all_removals
