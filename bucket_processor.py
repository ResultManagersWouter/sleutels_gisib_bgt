from buckets import BucketsBase, BucketsVRH
import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def only_match_id(df, gisib_id_col, bgt_id_col):
    return df[[gisib_id_col, bgt_id_col]].copy()


def match_id_and_remove(df, gisib_id_col, bgt_id_col):
    df_result = df[[gisib_id_col, bgt_id_col]].copy()
    df_remove = (
        df.assign(area=lambda df: df.geometry.area)
        .sort_values(by="area", ascending=False)
        .loc[lambda df: df.duplicated(subset=[bgt_id_col]), [gisib_id_col]]
    )

    mask = ~df_result[gisib_id_col].isin(df_remove[gisib_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, df_remove


def match_id_and_add(df, gisib_id_col, bgt_id_col, gisib_df):
    if "area" not in df.columns:
        df = df.assign(area=df.geometry.area)

    df_result = df[[gisib_id_col, bgt_id_col]].copy()

    df_add = (
        df.assign(area=lambda df: df.geometry.area)
        .sort_values(by="area", ascending=False)
        .loc[df.duplicated(subset=[gisib_id_col]), [gisib_id_col, bgt_id_col, "geometry_bgt"]]
    )

    add_objects = (
        gisib_df.drop(columns=["geometry"])
        .merge(df_add, how="inner", left_on=gisib_id_col, right_on=gisib_id_col)
        .set_geometry("geometry_bgt")
        .rename_geometry("geometry")
        .set_crs("EPSG:28992")
    )

    # Remove unwanted columns
    columns_to_remove = {
        "IDENTIFICATIE", "IMGEOID", "VRH_ID", "GRN_ID", "TRD_ID", "ID", "GUID"
    }
    cols_to_drop = [col for col in columns_to_remove if col in add_objects.columns]
    if cols_to_drop:
        add_objects = add_objects.drop(columns=cols_to_drop)

    # Add empty GUID column
    add_objects["GUID"] = pd.NA  # or use None if preferred

    mask = ~df_result[bgt_id_col].isin(df_add[bgt_id_col])
    result_df = df_result.loc[mask, [gisib_id_col, bgt_id_col]]

    return result_df, add_objects



BUCKET_MATCH_CONFIG = {
    BucketsBase.BUCKET1.value: {"function": only_match_id, "mode": "only"},   # geom_1_to_1
    BucketsBase.BUCKET2.value: {"function": match_id_and_remove, "mode": "remove"},  # gisib_merge
    BucketsBase.BUCKET4.value: {"function": match_id_and_add, "mode": "add"},       # gisib_split
    BucketsBase.BUCKET5.value: {"function": only_match_id, "mode": "only"},         # geom_75_match
    BucketsVRH.BUCKET6.value: {"function": only_match_id, "mode": "only"},          # geom_overlap_150_match (for VRH)
}



import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def process_and_export_per_asset_mode(
    filtered_auto_buckets: dict[str, dict],
    gisib_datasets: dict[str, pd.DataFrame],
    gisib_id_col: str,
    bgt_id_col: str,
    output_dir: str
):
    os.makedirs(output_dir, exist_ok=True)
    all_additions = {}
    all_removals = {}

    for asset, bucket_dict in filtered_auto_buckets.items():
        logger.info(f"Processing asset: {asset}")
        match_rows = []
        add_rows = []
        remove_rows = []

        for bucket_enum, df in bucket_dict.items():
            bucket_label = bucket_enum  # assuming this is a string
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
            elif mode == "remove":
                result_df, remove_df = match_function(df, gisib_id_col, bgt_id_col)
                if not remove_df.empty:
                    remove_rows.append(remove_df[[gisib_id_col]])
            elif mode == "only":
                result_df = match_function(df, gisib_id_col, bgt_id_col)
            else:
                logger.error(f"Unsupported mode '{mode}' for bucket '{bucket_label}'")
                continue

            match_rows.append(result_df[[gisib_id_col, bgt_id_col]])

        # Save results to Excel per asset
        asset_excel_path = os.path.join(output_dir, f"matched_{asset}.xlsx")
        with pd.ExcelWriter(asset_excel_path) as writer:
            if match_rows:
                pd.concat(match_rows, ignore_index=True).to_excel(writer, sheet_name="match", index=False)
            if add_rows:
                combined_adds = pd.concat(add_rows, ignore_index=True)
                combined_adds.to_excel(writer, sheet_name="additions", index=False)
                all_additions[asset] = combined_adds
            if remove_rows:
                combined_removes = pd.concat(remove_rows, ignore_index=True)
                combined_removes.to_excel(writer, sheet_name="removes", index=False)
                all_removals[asset] = combined_removes

        logger.info(f"✅ Exported results for '{asset}' to {asset_excel_path}")

    return all_additions, all_removals
