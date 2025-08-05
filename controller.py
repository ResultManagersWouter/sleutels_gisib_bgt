from enums import AssetType
from matchers import VerhardingenMatcher, GroenobjectenMatcher, TerreindelenMatcher
from geopandas import GeoDataFrame
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

    def create_buckets(self, verbose: bool = False):
        buckets = {}
        for asset_name, matcher_class in ASSET_MATCHER_CLASSES.items():

            if asset_name.value in self.assets:
                matcher = matcher_class(
                    gisib_gdf=self.assets[asset_name.value],
                    bgt_gdf=self.bgt,
                    gisib_id_col=self.gisib_id_col,
                    bgt_id_col=self.bgt_id_col,
                    gisib_hoogteligging_col=self.gisib_hoogteligging_col,
                    bgt_hoogteligging_col=self.bgt_hoogteligging_col,
                )
                buckets[asset_name.value] = matcher.run()
        self.created_buckets = buckets
        return buckets

    def write_buckets_to_geopackages(self, suffix: str, directory: str = "."):
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
                print(asset_name, bucket_name, bucket_gdf.shape)
                if bucket_gdf.empty:
                    continue

                try:
                    bgt_ids = bucket_gdf[self.bgt_id_col].dropna().unique()
                    bgt_layer = self.bgt[self.bgt[self.bgt_id_col].isin(bgt_ids)]
                except KeyError:
                    logging.warning("Remaining bucket has no lokaalid")
                    bgt_layer = None  # or set to None if preferred
                if bgt_layer is not None and not bgt_layer.empty:
                    bgt_layer.to_file(
                        filename, layer=f"bgt - {bucket_name}", driver="GPKG"
                    )
                    layers_written += 1

                # Filter GISIB features
                gisib_ids = bucket_gdf[self.gisib_id_col].dropna().unique()
                gisib_layer = asset_gisib_gdf[
                    asset_gisib_gdf[self.gisib_id_col].isin(gisib_ids)
                ]
                if not gisib_layer.empty:
                    gisib_layer.to_file(
                        filename, layer=f"gisib - {bucket_name}", driver="GPKG"
                    )
                    layers_written += 1

            print(f"Written {layers_written} layers to {filename} for {asset_name}.")

    def filtered_buckets(
        self, bucket_type: str = "manual", automatic_bucket_values: list[str] = None
    ):
        if not self.created_buckets:
            results = self.create_buckets()
        else:
            results = self.created_buckets

        if automatic_bucket_values is None:
            automatic_bucket_values = []

        filtered_results = {}

        for asset_name, buckets in results.items():
            if bucket_type == "manual":
                filtered = {
                    bucket_name: gdf
                    for bucket_name, gdf in buckets.items()
                    if bucket_name not in automatic_bucket_values and not gdf.empty
                }
            elif bucket_type == "automatic":
                filtered = {
                    bucket_name: gdf
                    for bucket_name, gdf in buckets.items()
                    if bucket_name in automatic_bucket_values and not gdf.empty
                }
            else:
                raise ValueError("bucket_type must be 'manual' or 'automatic'")

            if filtered:
                filtered_results[asset_name] = filtered

        return filtered_results

    def write_manual_buckets_to_geopackages(
        self,
        suffix: str,
        directory: str = ".",
        automatic_bucket_values: list[str] = None,
    ):
        """
        Writes only manual buckets (those NOT in automatic_bucket_values) to GeoPackages.

        Args:
            suffix: Suffix for the output filename.
            directory: Output directory (created if it doesn't exist).
            automatic_bucket_values: List of bucket value strings (e.g. "geom_1_to_1") that are automatic.
                                     Only buckets NOT in this list will be written.
        """
        if not self.created_buckets:
            results = self.create_buckets()
        else:
            results = self.created_buckets

        os.makedirs(directory, exist_ok=True)

        if automatic_bucket_values is None:
            raise ValueError("You must provide the list of automatic bucket values.")

        automatic_bucket_values_set = set(automatic_bucket_values)

        for asset_name, buckets in results.items():
            filename = os.path.join(directory, f"{asset_name}_{suffix}.gpkg")
            asset_gisib_gdf = self.assets[asset_name]
            layers_written = 0

            for bucket_name, bucket_gdf in buckets.items():
                if bucket_name in automatic_bucket_values_set or bucket_gdf.empty:
                    continue  # Skip automatic or empty buckets

                try:
                    bgt_ids = bucket_gdf[self.bgt_id_col].dropna().unique()
                    bgt_layer = self.bgt[self.bgt[self.bgt_id_col].isin(bgt_ids)]
                except KeyError:
                    logging.warning(f"Bucket {bucket_name} has no {self.bgt_id_col}")
                    bgt_layer = None

                if bgt_layer is not None and not bgt_layer.empty:
                    bgt_layer.to_file(
                        filename, layer=f"bgt - {bucket_name}", driver="GPKG"
                    )
                    layers_written += 1

                gisib_ids = bucket_gdf[self.gisib_id_col].dropna().unique()
                gisib_layer = asset_gisib_gdf[
                    asset_gisib_gdf[self.gisib_id_col].isin(gisib_ids)
                ]
                if not gisib_layer.empty:
                    gisib_layer.to_file(
                        filename, layer=f"gisib - {bucket_name}", driver="GPKG"
                    )
                    layers_written += 1

    def match_gisib_bgt_ids(self, suffix: str, directory: str = "."):
        if not self.created_buckets:
            results = self.create_buckets()
        else:
            results = self.created_buckets

        os.makedirs(directory, exist_ok=True)
        bucket_processor = BucketProcessor(
            bgt_id_col=self.bgt_id_col, gisib_id_col=self.gisib_id_col
        )

        for asset_name, buckets in results.items():
            asset_enum = AssetType(asset_name)
            filename = os.path.join(
                directory, f"{asset_enum.value}_{suffix}_matches.xlsx"
            )
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
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
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
