# This is a sample Python script.
import os
import logging
from dataloaders import read_gisib, read_bgt_shapes, read_controle_tabel
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from enums import AssetType, Gebied, ControleTabelGisib, ObjectType
from matchers import GroenobjectenMatcher, TerreindelenMatcher, VerhardingenMatcher
from columns_config import BGT_SHAPE_COLUMNS,ASSET_SCHEMAS,CONTROLE_TABEL_COLUMNS
from controller import Controller
from datetime import date

logger = logging.getLogger(__name__)

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

gisib_id_col = "GUID"
bgt_id_col = "lokaalid"
gisib_hoogteligging_col = "RELATIEVE_HOOGTELIGGING"
gisib_objecttype_col = "OBJECTTYPE"
today = date.today().isoformat()


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # controle tabel
    level = Gebied.BUURT.value
    controle_tabel = read_controle_tabel(
        filepath=os.environ.get("FP_CONTROLE_TABEL"),
        columns=CONTROLE_TABEL_COLUMNS,
        filterEnum=ControleTabelGisib,
        filter_col=ObjectType.CONTROLE_TABEL_GISIB_OBJECT.value,
    )

    # based on the controletabel the bgt types, we are going to filter on the shape file
    objecttypes_bgt = controle_tabel.loc[:,ObjectType.CONTROLE_TABEL_BGT_OBJECT.value].unique().tolist()
    # bgt = read_bgt(fp_bgt=os.environ.get('FP_BGT'),columns=BGT_COLUMNS)
    bgt = read_bgt_shapes(
        os.environ.get("FP_BGT_FOLDER"),
        columns = BGT_SHAPE_COLUMNS,
        objecttypes=objecttypes_bgt,
        object_col=ObjectType.BGTOBJECTTYPE.value

    )
    # make it a tuple for input
    bgt_bbox = tuple(bgt.total_bounds.tolist())

    # Load assets
    assets = {
        AssetType.TERREINDEEL.value: read_gisib(
            fp_gisib=os.environ.get("FP_TRD"),
            columns=ASSET_SCHEMAS[AssetType.TERREINDEEL],
            layer=AssetType.TERREINDEEL,
            bbox=bgt_bbox
        ),
        AssetType.GROENOBJECTEN.value: read_gisib(
            fp_gisib=os.environ.get("FP_GRN"),
            columns=ASSET_SCHEMAS[AssetType.GROENOBJECTEN],
            layer=AssetType.GROENOBJECTEN,
            bbox=bgt_bbox
        ),
        AssetType.VERHARDINGEN.value: read_gisib(
            fp_gisib=os.environ.get("FP_VRH"),
            columns=ASSET_SCHEMAS[AssetType.VERHARDINGEN],
            layer=AssetType.VERHARDINGEN,
            bbox=bgt_bbox
        ),
    }

    #
    # filtered_assets = {
    #     key: df[df[level] == area].copy()
    #     for key, df in assets.items()
    # }
    # check if there is overlap in gisib
    validator = GisibValidator(
        assets=assets,
        gisib_id_col=gisib_id_col,
        relatieve_hoogteligging_col=gisib_hoogteligging_col,
        objecttype_col=gisib_objecttype_col,
        gpkg_path=f"{today}_overlaps_{level.lower()}.gpkg",
    )
    valid = validator.run_all_validations()
    # # if there is no overlap, continue
    # if valid.empty:
    #     controller = Controller(
    #         assets=assets,
    #         bgt=bgt,
    #         gisib_id_col=gisib_id_col,
    #         bgt_id_col=bgt_id_col,
    #         gisib_hoogteligging_col=gisib_hoogteligging_col,
    #         bgt_hoogteligging_col="hoogtelig",
    #     )
    #     buckets = controller.create_buckets()
    #     if buckets:
    #         controller.match_gisib_bgt_ids(suffix=area.lower(),directory=date.today().isoformat() +"_" +area.lower())
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
