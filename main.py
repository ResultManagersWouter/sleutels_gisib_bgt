# This is a sample Python script.
import os
import logging
from datetime import date
from dataloaders import load_assets, read_bgt_shapes, read_controle_tabel, read_gebied
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from enums import ControleTabelGisib, ObjectType
import global_vars
from columns_config import (
    BGT_SHAPE_COLUMNS,
    column_mapping_bgt_controle_tabel,
    CONTROLE_TABEL_COLUMNS,
EXPORT_INVALID_TYPE_COMBINATIONS
)
from controller import Controller
from controller_utils import (
    should_process_buckets,
    get_invalid_combinations_by_control_table,
)
from buckets import ALL_AUTOMATIC_BUCKETS
from bucket_processor import process_and_export_per_asset_mode
from validate_output import validate_excel_matches
from gebieden import gebieden

# user input:
input_gebieden = [
    # 'Centrum',
 # 'Nieuw-West',
 # 'Noord',
 # 'Oost',
 # 'Weesp',
 # 'West',
 # 'Westpoort',
 # 'Zuid',
 'Zuidoost'
]

# negate = False = intersection met de input gebieden
# negate = True = alles behalve de intersection met input gebieden
negate = False


create_manual_buckets = True
write_manual_buckets = False
make_import_files = False

# WARNING: Make sure BGT has the same data as gisib.
assert all([gebied in gebieden for gebied in input_gebieden]), "One or more gebieden are missing"
# check of alle gebieden voorkomen in hetzelfde attribuut:
# zijn alle gebieden of allemaal stadsdelen, of allemaal buurten.
assert len(set([gebieden[gebied] for gebied in input_gebieden])) == 1
gebied_col = gebieden[input_gebieden[0]]
# negate = False, dus het is intersect.negate=True alles behalve intersect


logger = logging.getLogger(__name__)

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # deze wil je niet kunnen inverse/ontkennen dit wil je alleen bij de objecten doen.
    filter_polygon = read_gebied(os.environ.get("FP_GEBIEDEN"), gebieden=input_gebieden)
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
        filter_polygon=filter_polygon,
        negate = negate
    )
    # Load assets
    assets = load_assets(
        filter_polygon=filter_polygon, gebied_col=gebied_col, gebieden=input_gebieden,negate=negate
    )

    validator = GisibValidator(
        assets=assets,
        gisib_id_col=global_vars.gisib_id_col,
        relatieve_hoogteligging_col=global_vars.gisib_hoogteligging_col,
        objecttype_col=global_vars.gisib_objecttype_col,
        gpkg_path=f"{global_vars.today}_overlaps_{'_'.join(input_gebieden).lower()}.gpkg",
    )
    # hierin staan de overlappingen geodataframe
    overlaps_gisib = validator.run_all_validations()

    # matcher = GroenobjectenMatcher(gisib_gdf = assets["groenobjecten"],
    #                       bgt_gdf = bgt,
    #                                gisib_id_col=global_vars.gisib_id_col,
    #                                bgt_id_col=global_vars.bgt_id_col,
    #                                gisib_hoogteligging_col=global_vars.gisib_hoogteligging_col,
    #                                bgt_hoogteligging_col=global_vars.bgt_hoogteligging_col
    #                                )
    # overlap = matcher.calculate_overlap_df()

    # if there is no overlap, continue

    if overlaps_gisib.empty or create_manual_buckets:
    # if True:
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
        # check of er nog manuele handelingen nodig zijn, zo nee return False
        # Als er nog wat moet gebeuren is process_required = True, en gaat die niet verder.
        process_required = should_process_buckets(
            buckets_to_process,
            type_col=global_vars.TYPE_COL_GISIB,
            skip_types=global_vars.SKIP_TYPES,
        )

        # I have checked them
        # process_required = False

        # hier schrijf je de buckets automatisch weg die manueel beoordeeld moeten worden?
        if write_manual_buckets:
            controller.write_manual_buckets_to_geopackages(suffix="manual", directory=f"f{global_vars.today}_{'_'.join(input_gebieden).lower()}",
                                                       automatic_bucket_values=automatic_buckets)

        if not process_required or make_import_files:
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
            if not invalid_type_combinations and make_import_files:
                output_dir = f"output/{'_'.join()}_{global_vars.today}".replace(
                    " ", "_"
                )

                # Create the directory
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Created output directory: {output_dir}")
                # Load assets
                assets_all_columns = load_assets(
                    filter_polygon=filter_polygon, gebied_col=gebied_col, gebieden=input_gebieden, negate=negate,use_schema_columns=False
                )

                process_and_export_per_asset_mode(
                    filtered_auto_buckets=filtered_auto_buckets,
                    gisib_datasets=assets_all_columns,
                    gisib_id_col=global_vars.gisib_id_col,
                    bgt_id_col=global_vars.bgt_id_col,
                    output_dir=output_dir,
                )

                # check if the output is correct
                validate_excel_matches(
                    output_dir=output_dir,
                    asset_all_columns=assets_all_columns,
                    buckets_to_process=buckets_to_process,
                    invalid_type_combinations=invalid_type_combinations,
                    gisib_id_col=global_vars.gisib_id_col,
                    bgt_id_col=global_vars.bgt_id_col
                )
            # end
