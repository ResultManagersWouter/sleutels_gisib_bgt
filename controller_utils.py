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

logger = logging.getLogger(__name__)

def get_invalid_combinations_by_control_table(
    buckets: dict[str, dict[str, pd.DataFrame]],
    control_df: pd.DataFrame,
    guid_column: str,
    bgt_column: str,
    overlap_bgt_column: str,
    overlap_gisib_column: str,
    verbose: bool = False
) -> tuple[dict[str, list[dict]], dict[str, dict[str, pd.DataFrame]]]:
    """
    Returns:
    - A dict mapping each asset (e.g. "verhardingen") to a list of dicts with keys:
        - 'guid': GUID of the invalid object
        - 'lokaalid': LOKAALID of the object
        - 'min_overlap': min(overlap_bgt, overlap_gisib)
    - A dict of filtered dataframes with only valid rows per asset and category (i.e. invalid ones are removed).
    """
    columns_to_check = control_df.columns.tolist()

    # Convert control table into a set of tuples (with None for NaN)
    control_combos = set(
        tuple(None if pd.isna(val) else val for val in row)
        for row in control_df[columns_to_check].itertuples(index=False, name=None)
    )

    invalid_by_asset = {}
    filtered_buckets = {}

    for asset, categories in buckets.items():
        invalid_entries = []
        valid_categories = {}

        for category_name, df in categories.items():
            if df.empty:
                valid_categories[category_name] = df
                continue

            row_combos = [
                tuple(None if pd.isna(val) else val for val in row)
                for row in df[columns_to_check].itertuples(index=False, name=None)
            ]

            is_valid = [combo in control_combos for combo in row_combos]
            valid_indices = [i for i, valid in enumerate(is_valid) if valid]
            invalid_indices = [i for i, valid in enumerate(is_valid) if not valid]

            valid_df = df.iloc[valid_indices]
            valid_categories[category_name] = valid_df

            if invalid_indices:
                for i in invalid_indices:
                    row = df.iloc[i]
                    guid = row.get(guid_column, None)
                    lokaalid = row.get(bgt_column, None)
                    overlap_bgt = row.get(overlap_bgt_column, None)
                    overlap_gisib = row.get(overlap_gisib_column, None)

                    try:
                        min_overlap = min(float(overlap_bgt), float(overlap_gisib))
                    except (TypeError, ValueError):
                        min_overlap = None

                    entry = {
                        "guid": str(guid) if pd.notna(guid) else None,
                        "lokaalid": str(lokaalid) if pd.notna(lokaalid) else None,
                        "min_overlap": min_overlap
                    }
                    invalid_entries.append(entry)

                    if verbose:
                        combo_dict = {
                            col: (None if pd.isna(row[col]) else row[col])
                            for col in columns_to_check
                        }
                        logger.info(
                            f"[INVALID] Asset='{asset}', Category='{category_name}', GUID='{guid}': "
                            f"Values={combo_dict}, Min Overlap={min_overlap}"
                        )

        if invalid_entries:
            invalid_by_asset[asset] = invalid_entries

        filtered_buckets[asset] = valid_categories

    return invalid_by_asset, filtered_buckets
