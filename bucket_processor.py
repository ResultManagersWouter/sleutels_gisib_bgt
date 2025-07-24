from buckets import BucketsBase, BucketsVRH
import logging
from asset_config import AssetType

logger = logging.getLogger(__name__)

ASSET_BUCKET_ENUM = {
    AssetType.TERREINDEEL: BucketsBase,
    AssetType.GROENOBJECTEN: BucketsBase,
    AssetType.VERHARDINGEN: BucketsVRH,
}

class BucketProcessor:
    def __init__(self, bgt_id_col: str, gisib_id_col: str):
        self.bgt_id_col = bgt_id_col
        self.gisib_id_col = gisib_id_col

        # other actions instead of matches
        self.bucket2_remove_objects = {}
        self.bucket4_new_objects = {}

        # Build dispatch table for (asset, bucket) -> function
        self.dispatch = {
            AssetType.TERREINDEEL: {
                BucketsBase.BUCKET1: self.process_bucket1,
                BucketsBase.BUCKET2: self.process_bucket2,
                BucketsBase.BUCKET4: self.process_bucket4,
                BucketsBase.BUCKET5: self.process_bucket5,
            },
            AssetType.GROENOBJECTEN: {
                BucketsBase.BUCKET1: self.process_bucket1,
                BucketsBase.BUCKET2: self.process_bucket2,
                BucketsBase.BUCKET4: self.process_bucket4,
                BucketsBase.BUCKET5: self.process_bucket5,
            },
            AssetType.VERHARDINGEN: {
                BucketsVRH.BUCKET1: self.process_bucket1,
                BucketsVRH.BUCKET2: self.process_bucket2,
                BucketsVRH.BUCKET4: self.process_bucket4,
                BucketsVRH.BUCKET5: self.process_bucket5,
                BucketsVRH.BUCKET6: self.process_bucket6_vrh,
            },
        }

    def process(self, asset, bucket, results):
        if asset in self.dispatch and bucket in self.dispatch[asset]:
            return self.dispatch[asset][bucket](results, asset, bucket)
        else:
            logger.warning(f"No function for asset '{asset}' and bucket '{bucket}'")

    def process_bucket1(self, results, asset, bucket):
        logger.info(f"{asset}: Processing BUCKET1 ({bucket.value})")
        df = results[asset][bucket.value]
        if not df.empty:
            return df.loc[:, [self.gisib_id_col, self.bgt_id_col]]
        else:
            logger.info(f"{asset}: EMPTY BUCKET1: ({bucket.value})")

    def process_bucket2(self, results, asset, bucket):
        logger.info(f"{asset}: Processing BUCKET2 ({bucket.value})")
        df = results[asset][bucket.value]
        if not df.empty:
            df = df.assign(area=lambda df: df.geometry.area).sort_values(by=[self.bgt_id_col, "area"])
            df_keep = df.loc[~df.duplicated(subset=[self.bgt_id_col], keep="first"), [self.gisib_id_col, self.bgt_id_col]]
            self.bucket2_remove_objects[asset] = df.loc[df.duplicated(subset=[self.bgt_id_col], keep="first"), [self.gisib_id_col]]
            return df_keep
        else:
            logger.info(f"{asset}: EMPTY BUCKET2: ({bucket.value})")

    def process_bucket4(self, results, asset, bucket):
        logger.info(f"{asset}: Processing BUCKET4 ({bucket.value})")
        df = results[asset][bucket.value]
        if not df.empty:
            df_match = (
                df
                .sort_values(by=[self.gisib_id_col, "overlap_gisib"], ascending=False)
                .loc[~df.duplicated(subset=[self.gisib_id_col]),
                [self.gisib_id_col, self.bgt_id_col]]
            )
            self.bucket4_new_objects[asset] = (
                df.loc[~df[self.bgt_id_col].isin(df_match[self.bgt_id_col].tolist()), [self.gisib_id_col, self.bgt_id_col]]
            )
            return df_match
        else:
            logger.info(f"{asset}: EMPTY BUCKET4: ({bucket.value})")

    def process_bucket5(self, results, asset, bucket):
        logger.info(f"{asset}: Processing BUCKET5 ({bucket.value})")
        df = results[asset][bucket.value]
        if not df.empty:
            return df.loc[:, [self.gisib_id_col, self.bgt_id_col]]
        else:
            logger.info(f"{asset}: EMPTY BUCKET5: ({bucket.value})")

    def process_bucket6_vrh(self, results, asset, bucket):
        logger.info(f"{asset}: Processing EXTRA BUCKET6 ({bucket.value})")
        df = results[asset][bucket.value]
        if not df.empty:
            return df.loc[:, [self.gisib_id_col, self.bgt_id_col]]
        else:
            logger.info(f"{asset}: EMPTY BUCKET6 VRH: ({bucket.value})")
