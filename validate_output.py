import pandas as pd
import logging
import os
logger = logging.getLogger(__name__)
import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def validate_excel_matches(
    output_dir: str,
    asset_all_columns: dict[str, pd.DataFrame],
    buckets_to_process: dict[str, dict],
    invalid_type_combinations: dict[str, list[dict]],
    gisib_id_col: str,
    bgt_id_col: str
):
    all_guid_handled = set()
    all_guid_input = set()
    all_guid_sources = []
    all_lokaalids = []
    duplicate_lokaalids_global = set()
    duplicate_guids_global = set()

    for asset, input_df in asset_all_columns.items():
        file_path = os.path.join(output_dir, f"matched_{asset}.xlsx")
        if not os.path.exists(file_path):
            logger.error(f"Missing Excel file for asset: {asset}")
            continue

        try:
            xl = pd.ExcelFile(file_path)
        except Exception as e:
            logger.error(f"[{asset}] Could not open Excel file: {e}")
            continue

        def safe_read(sheet):
            if sheet in xl.sheet_names:
                try:
                    return xl.parse(sheet)
                except Exception as e:
                    logger.warning(f"[{asset}] Failed to read sheet '{sheet}': {e}")
            return pd.DataFrame()

        df_match = safe_read("match")
        if all(col in df_match.columns for col in [gisib_id_col, bgt_id_col]):
            df_match = df_match.dropna(subset=[gisib_id_col, bgt_id_col])
        else:
            if not df_match.empty:
                logger.warning(f"[{asset}] 'match' sheet missing columns: {gisib_id_col} and/or {bgt_id_col}")
            df_match = pd.DataFrame()

        df_add = safe_read("add")
        if bgt_id_col in df_add.columns:
            df_add = df_add.dropna(subset=[bgt_id_col])
        else:
            if not df_add.empty:
                logger.warning(f"[{asset}] 'add' sheet missing column: {bgt_id_col}")
            df_add = pd.DataFrame()

        df_remove = safe_read("remove")
        if gisib_id_col in df_remove.columns:
            df_remove = df_remove.dropna(subset=[gisib_id_col])
        else:
            if not df_remove.empty:
                logger.warning(f"[{asset}] 'remove' sheet missing column: {gisib_id_col}")
            df_remove = pd.DataFrame()

        # GUIDs from full input
        guids_input = set(input_df[gisib_id_col].dropna())
        all_guid_input.update(guids_input)

        # Handled GUIDs
        guids_matched = set(df_match[gisib_id_col]) if gisib_id_col in df_match else set()
        guids_removed = set(df_remove[gisib_id_col]) if gisib_id_col in df_remove else set()
        guids_manual = set()
        for df in buckets_to_process.get(asset, {}).values():
            if gisib_id_col in df.columns:
                guids_manual.update(df[gisib_id_col].dropna())
        guids_handled = guids_matched | guids_removed | guids_manual
        all_guid_handled.update(guids_handled)
        all_guid_sources.extend([*guids_matched, *guids_removed, *guids_manual])

        # Check for GUID duplicates
        guid_series = pd.Series(all_guid_sources)
        guid_dupes = guid_series[guid_series.duplicated()].unique().tolist()
        if guid_dupes:
            logger.warning(f"[{asset}] 🔴 Duplicate GUIDs found in match/remove/buckets: {guid_dupes}")
            duplicate_guids_global.update(guid_dupes)

        # Lokaalids from match + add
        lokaalids = pd.concat([
            df_match[bgt_id_col] if bgt_id_col in df_match else pd.Series(dtype="object"),
            df_add[bgt_id_col] if bgt_id_col in df_add else pd.Series(dtype="object")
        ]).dropna()
        all_lokaalids.extend(lokaalids)
        duplicates_local = lokaalids[lokaalids.duplicated()].unique().tolist()
        if duplicates_local:
            logger.warning(f"[{asset}] 🔴 Duplicate lokaalids in match/add: {duplicates_local}")
            duplicate_lokaalids_global.update(duplicates_local)

        # Unhandled input GUIDs
        unhandled = guids_input - guids_handled
        logger.info(f"[{asset}] Input GUIDs: {len(guids_input)} | Handled: {len(guids_input) - len(unhandled)} | Unhandled: {len(unhandled)}")
        logger.info(
            f"[{asset}] Breakdown: matched={len(guids_matched)}, removed={len(guids_removed)}, "
            f"invalid_type_combinations={len(invalid_type_combinations.get(asset, []))}, "
            f"buckets_to_process={len(guids_manual)}, added={len(df_add) if not df_add.empty else 0}"
        )
        if unhandled:
            for guid in list(unhandled)[:5]:
                logger.warning(f"[{asset}] Example unhandled GUID: {guid}")
        else:
            logger.info(f"[{asset}] ✅ All input GUIDs are handled.")

        # Track invalid type combinations status
        invalid_entries = invalid_type_combinations.get(asset, [])
        guids_invalid = {entry[gisib_id_col] for entry in invalid_entries if gisib_id_col in entry}
        invalid_handled = [guid for guid in guids_invalid if guid in guids_matched or guid in guids_removed]
        invalid_unhandled = guids_invalid - guids_handled
        logger.info(f"[{asset}] invalid_type_combinations (total): {len(invalid_entries)}")
        logger.info(f"[{asset}] invalid_type_combinations status: handled={len(invalid_handled)}, still_to_process={len(invalid_unhandled)}")
        if invalid_unhandled:
            for guid in list(invalid_unhandled)[:5]:
                logger.warning(f"[{asset}] Invalid combination not yet processed: {guid}")

    # Global summary
    logger.info("🔍 Global summary:")
    logger.info(f"  Total unique lokaalids (match + add): {len(set(all_lokaalids))}")
    logger.info(f"  Total handled GUIDs: {len(all_guid_handled)}")
    logger.info(f"  Total input GUIDs (from all assets): {len(all_guid_input)}")

    lokaalid_series = pd.Series(all_lokaalids)
    dupes_global = lokaalid_series[lokaalid_series.duplicated()].unique().tolist()
    if dupes_global:
        logger.error(f"🔴 Duplicate lokaalids found globally across assets: {dupes_global}")
    else:
        logger.info("✅ All lokaalids are globally unique across match/add sheets.")

    if duplicate_guids_global:
        logger.error(f"🔴 Duplicate GUIDs found across sources: {duplicate_guids_global}")
    else:
        logger.info("✅ All GUIDs are unique across match/remove/buckets.")
