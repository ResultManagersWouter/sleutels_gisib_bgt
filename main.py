# This is a sample Python script.
import os
import logging
from dataloaders import read_gisib,read_bgt,read_bgt_shapes,read_controle_tabel
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from enums import AssetType, Gebied,ControleTabelGisib,ObjectType
from matchers import GroenobjectenMatcher,TerreindelenMatcher,VerhardingenMatcher
from columns_config import BGT_COLUMNS,ASSET_SCHEMAS
from controller import Controller
from datetime import date
logger = logging.getLogger(__name__)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

gisib_id_col = "GUID"
bgt_id_col = "lokaalid"
gisib_hoogteligging_col = "RELATIEVE_HOOGTELIGGING"
gisib_objecttype_col = "OBJECTTYPE"


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # controle tabel
    controle_tabel = read_controle_tabel(filepath=os.environ.get("FP_CONTROLE_TABEL"),
                                         columns = ...,
                                         filterEnum=ControleTabelGisib,
                                         filter_col=ObjectType.CONTROLE_TABEL.value)

    # Load assets
    assets = {
        AssetType.TERREINDEEL: read_gisib(fp_gisib=os.environ.get('FP_TRD'),
                                          columns=ASSET_SCHEMAS[AssetType.TERREINDEEL],
                                          layer=AssetType.TERREINDEEL),
        AssetType.GROENOBJECTEN: read_gisib(fp_gisib=os.environ.get("FP_GRN"),
                                            columns=ASSET_SCHEMAS[AssetType.GROENOBJECTEN],
                                            layer=AssetType.GROENOBJECTEN),
        AssetType.VERHARDINGEN: read_gisib(fp_gisib=os.environ.get("FP_VRH"),
                                           columns=ASSET_SCHEMAS[AssetType.VERHARDINGEN],
                                           layer=AssetType.VERHARDINGEN),
    }

    # based on the controletabel
    objecttypes = ...
    # bgt = read_bgt(fp_bgt=os.environ.get('FP_BGT'),columns=BGT_COLUMNS)
    bgt = read_bgt_shapes(os.environ.get("FP_BGT_FOLDER"),)
    level = Gebied.BUURT.value
    area = "all"
    #
    # filtered_assets = {
    #     key: df[df[level] == area].copy()
    #     for key, df in assets.items()
    # }
    # check if there is overlap in gisib
    validator = GisibValidator(assets=assets,
                               gisib_id_col=gisib_id_col,
                               relatieve_hoogteligging_col=gisib_hoogteligging_col,
                               objecttype_col=gisib_objecttype_col,
                               gpkg_path=f"overlaps_{area.lower()}.gpkg")
    valid = validator.run_all_validations()
    # # if there is no overlap, continue
    if valid.empty:
        controller = Controller(
            assets=assets,
            bgt =bgt,
             gisib_id_col =gisib_id_col,
             bgt_id_col=bgt_id_col,
             gisib_hoogteligging_col=gisib_hoogteligging_col,
             bgt_hoogteligging_col="hoogtelig")
        buckets = controller.create_buckets()
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
