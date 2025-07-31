# This is a sample Python script.
import os
import logging
from dataloaders import load_assets, read_bgt_shapes, read_controle_tabel, read_gebied
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from enums import ControleTabelGisib, ObjectType
import global_vars
from columns_config import BGT_SHAPE_COLUMNS, ASSET_SCHEMAS, CONTROLE_TABEL_COLUMNS
from controller import Controller
from buckets import ALL_AUTOMATIC_BUCKETS

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
    )

    # based on the controletabel the bgt types, we are going to filter on the shape file
    objecttypes_bgt = (
        controle_tabel.loc[:, ObjectType.CONTROLE_TABEL_BGT_OBJECT.value]
        .unique()
        .tolist()
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
    assets = assets = load_assets(
    bbox=bbox,
    gebied_col=global_vars.gebied_col,
    gebied=global_vars.gebied
)

    validator = GisibValidator(
        assets=assets,
        gisib_id_col=global_vars.gisib_id_col,
        relatieve_hoogteligging_col=global_vars.gisib_hoogteligging_col,
        objecttype_col=global_vars.gisib_objecttype_col,
        gpkg_path=f"{global_vars.today}_overlaps_{global_vars.gebied.lower()}.gpkg",
    )
    valid = validator.run_all_validations()
    # # if there is no overlap, continue
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
        buckets = controller.manual_buckets(automatic_bucket_values=automatic_buckets)

        # if buckets:
        #     automatic_buckets = [bucket.value for bucket in ALL_AUTOMATIC_BUCKETS]
            # controller.write_buckets_to_geopackages(suffix=gebied.lower(),directory="./output/"+date.today().isoformat() +"_" +gebied.lower())
            # manual_buckets = controller.()
            # controller.write_manual_buckets_to_geopackages(suffix=gebied.lower(),
            #                                                directory="./output/"+date.today().isoformat() +"_" +gebied.lower(),
            #                                                automatic_bucket_values=automatic_buckets)
    # results = controller.run()

    # # Run pre-validation
    # # validator = GisibValidator(assets = filtered_assets,threshold = 0.5)
    # # check_overlaps = validator.run_all_validations()
    # gisib = VerhardingenMatcher(gisib_gdf=filtered_assets[AssetType.VERHARDINGEN.value],
    #                              bgt_gdf =bgt,
    #                              gisib_id_col = "guid",
    #                              bgt_id_col="lokaalid",
    #                              gisib_hoogteligging_col="relatieve_hoogteligging",
    #                              bgt_hoogteligging_col="hoogtelig")
    # # overlaps = groen.calculate_overlap_df()
    # # bgt_gisib = groen.build_bgt_gisib_grouped(intersection_df=overlaps)
    # intersection_df = gisib.preprocess()
    # buckets = gisib.run()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
