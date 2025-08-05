# This is a sample Python script.
import os
import logging
from dataloaders import load_assets, read_bgt_shapes, read_controle_tabel, read_gebied
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from enums import ControleTabelGisib, ObjectType
import global_vars
from columns_config import (
    BGT_SHAPE_COLUMNS,
    column_mapping_bgt_controle_tabel,
    CONTROLE_TABEL_COLUMNS,
)
from controller import Controller
from controller_utils import (
    should_process_buckets,
    get_invalid_combinations_by_control_table,
)
from buckets import ALL_AUTOMATIC_BUCKETS
from bucket_processor import process_and_export_per_asset_mode

logger = logging.getLogger(__name__)

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    bbox = read_gebied(os.environ.get("FP_GEBIEDEN"), gebied=global_vars.gebied)
    # controle tabel
    controle_tabel = read_controle_tabel(
        filepath=os.environ.get("FP_CONTROLE_TABEL"),
        columns=CONTROLE_TABEL_COLUMNS,
        filterEnum=ControleTabelGisib,
        filter_col=ObjectType.CONTROLE_TABEL_GISIB_OBJECT.value,
        mapping=column_mapping_bgt_controle_tabel,
    )

    # ControleTabel is mapped:
    # based on the controletabel the bgt types, we are going to filter on the shape file
    objecttypes_bgt = (
        controle_tabel.loc[:, ObjectType.BGTOBJECTTYPE.value].unique().tolist()
    )
    # bgt = read_bgt(fp_bgt=os.environ.get('FP_BGT'),columns=BGT_COLUMNS)
    bgt = read_bgt_shapes(
        os.environ.get("FP_BGT_FOLDER"),
        columns=BGT_SHAPE_COLUMNS,
        objecttypes=objecttypes_bgt,
        object_col=ObjectType.BGTOBJECTTYPE.value,
        bbox=bbox,
    )
    # Load assets
    assets = load_assets(
        bbox=bbox, gebied_col=global_vars.gebied_col, gebied=global_vars.gebied
    )

    validator = GisibValidator(
        assets=assets,
        gisib_id_col=global_vars.gisib_id_col,
        relatieve_hoogteligging_col=global_vars.gisib_hoogteligging_col,
        objecttype_col=global_vars.gisib_objecttype_col,
        gpkg_path=f"{global_vars.today}_overlaps_{global_vars.gebied.lower()}.gpkg",
    )
    valid = validator.run_all_validations()

    # matcher = GroenobjectenMatcher(gisib_gdf = assets["groenobjecten"],
    #                       bgt_gdf = bgt,
    #                                gisib_id_col=global_vars.gisib_id_col,
    #                                bgt_id_col=global_vars.bgt_id_col,
    #                                gisib_hoogteligging_col=global_vars.gisib_hoogteligging_col,
    #                                bgt_hoogteligging_col=global_vars.bgt_hoogteligging_col
    #                                )
    # overlap = matcher.calculate_overlap_df()

    # if there is no overlap, continue
    if valid.empty:
        controller = Controller(
            assets=assets,
            bgt=bgt,
            gisib_id_col=global_vars.gisib_id_col,
            bgt_id_col=global_vars.bgt_id_col,
            gisib_hoogteligging_col=global_vars.gisib_hoogteligging_col,
            bgt_hoogteligging_col=global_vars.bgt_hoogteligging_col,
        )

        automatic_buckets = [bucket.value for bucket in ALL_AUTOMATIC_BUCKETS]
        buckets_to_process = controller.filtered_buckets(
            bucket_type="manual", automatic_bucket_values=automatic_buckets
        )
        process_required = should_process_buckets(
            buckets_to_process,
            type_col=global_vars.TYPE_COL_GISIB,
            skip_types=global_vars.SKIP_TYPES,
        )

        # I have checked them
        # process_required = False

        if not process_required:
            auto_buckets = controller.filtered_buckets(
                bucket_type="automatic", automatic_bucket_values=automatic_buckets
            )
            invalid_type_combinations, filtered_auto_buckets = (
                get_invalid_combinations_by_control_table(
                    buckets=auto_buckets,
                    control_df=controle_tabel,
                    guid_column=global_vars.gisib_id_col,
                    bgt_column = global_vars.bgt_id_col,
                    overlap_bgt_column = "overlap_bgt",
                    overlap_gisib_column = "overlap_gisib",
                    verbose=True,
                )
            )
        if not invalid_type_combinations:
            output_dir = f"output/{global_vars.gebied}_{global_vars.today}".replace(
                " ", "_"
            )

            # Create the directory
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
            asset_all_columns = load_assets(
                bbox=bbox,
                gebied_col=global_vars.gebied_col,
                gebied=global_vars.gebied,
                use_schema_columns=False,
            )
            process_and_export_per_asset_mode(
                filtered_auto_buckets=filtered_auto_buckets,
                gisib_datasets=asset_all_columns,
                gisib_id_col=global_vars.gisib_id_col,
                bgt_id_col=global_vars.bgt_id_col,
                output_dir=output_dir,
            )

            def validate_excel_matches(
                    output_dir: str,
                    filtered_auto_buckets: dict[str, dict],
                    gisib_id_col: str,
                    bgt_id_col: str
            ):
                for asset, bucket_dict in filtered_auto_buckets.items():
                    file_path = os.path.join(output_dir, f"matched_{asset}.xlsx")
                    if not os.path.exists(file_path):
                        logger.error(f"Missing Excel file for asset: {asset}")
                        continue

                    try:
                        df_match = pd.read_excel(file_path, sheet_name="match")
                    except Exception as e:
                        logger.error(f"Could not read 'match' sheet in {file_path}: {e}")
                        continue

                    # Drop NA values just in case
                    df_match = df_match.dropna(subset=[gisib_id_col, bgt_id_col])

                    total_matches = len(df_match)
                    unique_gisib = df_match[gisib_id_col].nunique()
                    unique_bgt = df_match[bgt_id_col].nunique()

                    logger.info(
                        f"[{asset}] total matches: {total_matches}, unique {gisib_id_col}: {unique_gisib}, unique {bgt_id_col}: {unique_bgt}")

                    if total_matches != unique_gisib:
                        logger.warning(f"[{asset}] Duplicate {gisib_id_col} values found.")
                    if total_matches != unique_bgt:
                        logger.warning(f"[{asset}] Duplicate {bgt_id_col} values found.")

                    # Compare to input size
                    input_rows = sum(len(df) for df in bucket_dict.values())
                    if total_matches != input_rows:
                        logger.warning(
                            f"[{asset}] Not all objects are covered. Input: {input_rows}, Matched: {total_matches}")
                    else:
                        logger.info(f"[{asset}] ✅ All objects are covered.")


            validate_excel_matches(
                output_dir=output_dir,
                filtered_auto_buckets=filtered_auto_buckets,
                gisib_id_col=global_vars.gisib_id_col,
                bgt_id_col=global_vars.bgt_id_col
            )

            #         # start writing...


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
