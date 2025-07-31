import logging
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