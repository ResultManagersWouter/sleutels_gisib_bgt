# This is a sample Python script.
import os
import logging
from utils import read_gisib,read_bgt
from dotenv import load_dotenv
from gisib_validator import GisibValidator
from asset_config import AssetType, Gebied
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

column_map = {
    "guid": "GUID",
    "objecttype": "objecttype",
    "relatieve_hoogteligging": "relatieve_hoogteligging"
}


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
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

    # bgt = read_bgt(fp_bgt=os.environ.get('FP_BGT'),columns=BGT_COLUMNS)
    #
    # level = Gebied.BUURT.value
    # area = "all"
    # #
    # # filtered_assets = {
    # #     key: df[df[level] == area].copy()
    # #     for key, df in assets.items()
    # # }
    # # check if there is overlap in gisib
    # validator = GisibValidator(assets=assets,gisib_id_col="guid",gpkg_path=f"overlaps_{area.lower()}.gpkg")
    # valid = validator.run_all_validations()
    # # if there is no overlap, continue
    # if valid.empty:
    #     controller = Controller(
    #         assets=assets,
    #         bgt =bgt,
    #          gisib_id_col = "guid",
    #          bgt_id_col="lokaalid",
    #          gisib_hoogteligging_col="relatieve_hoogteligging",
    #          bgt_hoogteligging_col="hoogtelig")
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
