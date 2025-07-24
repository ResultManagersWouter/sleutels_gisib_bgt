from asset_config import AssetType
from matchers import VerhardingenMatcher,GroenobjectenMatcher,TerreindelenMatcher
from geopandas import GeoDataFrame
from bucket_processor import BucketProcessor,ASSET_BUCKET_ENUM
import logging
import pandas as pd
import os
logger = logging.getLogger(__name__)
ASSET_MATCHER_CLASSES = {
    AssetType.VERHARDINGEN: VerhardingenMatcher,
    AssetType.GROENOBJECTEN: GroenobjectenMatcher,
    AssetType.TERREINDEEL: TerreindelenMatcher,
}

class Controller:
    def __init__(
        self,
        assets: dict,
        bgt: GeoDataFrame,
        gisib_id_col: str,
        bgt_id_col: str,
        gisib_hoogteligging_col: str,
        bgt_hoogteligging_col: str,
    ):
        self.assets = assets
        self.bgt = bgt
        self.gisib_id_col = gisib_id_col
        self.bgt_id_col = bgt_id_col
        self.gisib_hoogteligging_col = gisib_hoogteligging_col
        self.bgt_hoogteligging_col = bgt_hoogteligging_col
        self.created_buckets = None

    def create_buckets(self):
        buckets = {}
        for asset_name, matcher_class in ASSET_MATCHER_CLASSES.items():
            if asset_name in self.assets:
                matcher = matcher_class(
                    gisib_gdf=self.assets[asset_name],
                    bgt_gdf=self.bgt,
                    gisib_id_col=self.gisib_id_col,
                    bgt_id_col=self.bgt_id_col,
                    gisib_hoogteligging_col=self.gisib_hoogteligging_col,
                    bgt_hoogteligging_col=self.bgt_hoogteligging_col,
                )
                buckets[asset_name] = matcher.run()
        self.created_buckets = buckets
        return buckets

    def write_overlaps_to_geopackages(self,suffix: str, directory: str = "."):
        """
        For each asset, writes each bucket (as two layers: _bgt and _gisib)
        to a GeoPackage with a name based on the asset_name and a user-provided suffix.
        Automatically creates the output directory if it does not exist.
        """
        if not self.created_buckets:
            results = self.create_buckets()
        else:
            results = self.created_buckets
        os.makedirs(directory, exist_ok=True)  # <-- ensures directory exists

        for asset_name, buckets in results.items():
            filename = os.path.join(directory, f"{asset_name}_{suffix}.gpkg")
            asset_gisib_gdf = self.assets[asset_name]
            layers_written = 0

            for bucket_name, bucket_gdf in buckets.items():
                if bucket_gdf.empty:
                    continue

                # Filter BGT features
                bgt_ids = bucket_gdf[self.bgt_id_col].dropna().unique()
                bgt_layer = self.bgt[self.bgt[self.bgt_id_col].isin(bgt_ids)]
                if not bgt_layer.empty:
                    bgt_layer.to_file(filename, layer=f"bgt - {bucket_name}", driver="GPKG")
                    layers_written += 1

                # Filter GISIB features
                gisib_ids = bucket_gdf[self.gisib_id_col].dropna().unique()
                gisib_layer = asset_gisib_gdf[asset_gisib_gdf[self.gisib_id_col].isin(gisib_ids)]
                if not gisib_layer.empty:
                    gisib_layer.to_file(filename, layer=f"gisib - {bucket_name}", driver="GPKG")
                    layers_written += 1


            print(f"Written {layers_written} layers to {filename} for {asset_name}.")

    def match_gisib_bgt_ids(self, suffix: str, directory: str = "."):
        if not self.created_buckets:
            results = self.create_buckets()
        else:
            results = self.created_buckets

        os.makedirs(directory, exist_ok=True)
        bucket_processor = BucketProcessor(
            bgt_id_col=self.bgt_id_col,
            gisib_id_col=self.gisib_id_col
        )

        for asset_name, buckets in results.items():
            asset_enum = AssetType(asset_name)
            filename = os.path.join(directory, f"{asset_enum.value}_{suffix}_matches.xlsx")
            match_rows = []
            dispatch_table = bucket_processor.dispatch.get(asset_enum, {})

            for bucket in dispatch_table.keys():
                try:
                    output = bucket_processor.process(asset_enum, bucket, results)
                    if output is not None and not output.empty:
                        match_rows.append(output)
                except Exception as e:
                    logger.warning(f"Failed processing {bucket} for {asset_enum}: {e}")

            # Prepare additional DataFrames
            remove_df = bucket_processor.bucket2_remove_objects.get(asset_enum)
            new_obj_df = bucket_processor.bucket4_new_objects.get(asset_enum)

            # Write everything to a single Excel file with multiple sheets
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main matches
                if match_rows:
                    all_matches_df = pd.concat(match_rows, ignore_index=True)
                    all_matches_df.to_excel(writer, sheet_name="matches", index=False)
                # To remove
                if remove_df is not None and not remove_df.empty:
                    remove_df.to_excel(writer, sheet_name="to_remove", index=False)
                # To add
                if new_obj_df is not None and not new_obj_df.empty:
                    new_obj_df.to_excel(writer, sheet_name="to_add", index=False)

            logger.info(f"Wrote Excel with matches, to_remove, to_add: {filename}")

