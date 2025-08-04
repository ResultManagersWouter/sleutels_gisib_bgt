import logging
import pandas as pd

logger = logging.getLogger(__name__)
def should_process_buckets(
        buckets: dict,
        skip_types: set = {"Rietland", "Moeras"},
        type_col: str = "TYPE"
) -> bool:
    """
    Checks if any non-empty, non-skippable buckets exist.

    Args:
        buckets (dict): Nested dict in the form {asset_name: {bucket_name: gdf}}
        skip_types (set): Set of TYPE values to skip
        type_col (str): Column name used to check type classification

    Returns:
        bool: True if at least one bucket should be processed, False otherwise.
    """
    buckets_to_process = []

    for asset_name, asset_buckets in buckets.items():
        for bucket_name, gdf in asset_buckets.items():
            if gdf.empty:
                continue

            types = set(gdf[type_col].dropna().unique())
            if types.issubset(skip_types):
                logger.info(f"{asset_name} - {bucket_name} - all values {types} are in {skip_types}")
                continue

            buckets_to_process.append((asset_name, bucket_name))

    if not buckets_to_process:
        logger.info("✅ All buckets are empty or skippable — skipping processing.")
        return False

    logger.info("🚧 Buckets to process:")
    for asset_name, bucket_name in buckets_to_process:
        logger.warning(f" - {asset_name} - {bucket_name}")

    return True


def get_invalid_combinations_by_control_table(
    buckets: dict[str, dict[str, pd.DataFrame]],
    control_df: pd.DataFrame,
    guid_column: str = "GUID"
) -> dict[str, list[str]]:
    """
    Returns a dict mapping each asset (e.g. "verhardingen") to a list of GUIDs
    that do not match any valid combination in the control_df.
    """
    sentinel = object()
    columns_to_check = control_df.columns
    control_filled = control_df[columns_to_check].fillna(sentinel)
    control_combos = set(zip(*[control_filled[col] for col in columns_to_check]))

    invalid_by_asset = {}

    for asset, categories in buckets.items():
        invalid_guids = []

        for category_name, df in categories.items():
            df_filled = df[columns_to_check].fillna(sentinel)
            df_combos = list(zip(*[df_filled[col] for col in columns_to_check]))

            is_valid = [combo in control_combos for combo in df_combos]
            invalid_rows = df.loc[[not ok for ok in is_valid]]

            invalid_guids.extend(invalid_rows[guid_column].dropna().astype(str).tolist())

        if invalid_guids:
            invalid_by_asset[asset] = list(set(invalid_guids))

    return invalid_by_asset