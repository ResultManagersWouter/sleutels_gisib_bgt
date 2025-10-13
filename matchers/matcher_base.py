from typing import Tuple, Dict, Set
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely import unary_union
import pandas as pd
import logging
import global_vars
from buckets import BucketsBase,AUTOMATIC_BUCKETS
logger = logging.getLogger(__name__)
class MatcherBase:
    """
    Provides advanced spatial matching and grouping of GISIB and BGT geospatial datasets.
    """

    def __init__(
        self,
        gisib_gdf: GeoDataFrame,
        bgt_gdf: GeoDataFrame,
        gisib_id_col: str,
        bgt_id_col: str,
        gisib_hoogteligging_col: str,
        bgt_hoogteligging_col: str,
        tolerance: float = 0.001,
        min_ratio: float = 0.10,
    ) -> None:
        """
        Initialize GroenMatcher with required input GeoDataFrames and configuration.
        """
        self._validate_init_args(
            gisib_gdf, bgt_gdf, gisib_id_col, bgt_id_col, gisib_hoogteligging_col, bgt_hoogteligging_col
        )
        # skip the types immediately
        self.gisib = gisib_gdf.copy().loc[lambda df: ~df.loc[:,global_vars.TYPE_COL_GISIB].isin(global_vars.SKIP_TYPES)]
        self.bgt = bgt_gdf.copy()
        self.gisib_id_col = gisib_id_col
        self.bgt_id_col = bgt_id_col
        self.gisib_hoogteligging_col = gisib_hoogteligging_col
        self.bgt_hoogteligging_col = bgt_hoogteligging_col
        self.tolerance = tolerance
        self.min_ratio = min_ratio

        # Harmonize CRS
        if self.gisib.crs != self.bgt.crs:
            self.bgt = self.bgt.to_crs(self.gisib.crs)

        # Guarantee custom geometry columns exist
        self.gisib["geometry_gisib"] = self.gisib.get("geometry_gisib", self.gisib.geometry)
        self.bgt["geometry_bgt"] = self.bgt.get("geometry_bgt", self.bgt.geometry)

    @staticmethod
    def _validate_init_args(
        gisib_gdf: GeoDataFrame,
        bgt_gdf: GeoDataFrame,
        gisib_id_col: str,
        bgt_id_col: str,
        gisib_hoogteligging_col: str,
        bgt_hoogteligging_col: str,
    ):
        """
        Validates inputs for GroenMatcher initialization.
        """
        for df, label in [(gisib_gdf, "gisib_gdf"), (bgt_gdf, "bgt_gdf")]:
            if not isinstance(df, GeoDataFrame):
                raise TypeError(f"{label} must be a GeoDataFrame")
        for col, df, label in [
            (gisib_id_col, gisib_gdf, "gisib_id_col"),
            (gisib_hoogteligging_col, gisib_gdf, "gisib_hoogteligging_col"),
            (bgt_id_col, bgt_gdf, "bgt_id_col"),
            (bgt_hoogteligging_col, bgt_gdf, "bgt_hoogteligging_col"),
        ]:
            if col not in df.columns:
                raise ValueError(f"{label} '{col}' not found in corresponding dataframe columns")

    def calculate_overlap_df(self) -> GeoDataFrame:
        """
        Compute intersection overlay between GISIB and BGT polygons.
        Returns:
            GeoDataFrame with overlap ratios and geometry equality flags.
        """
        overlay_df = self.gisib.overlay(
            self.bgt, how="intersection", keep_geom_type=True
        )

        if not {"geometry_gisib", "geometry_bgt"}.issubset(overlay_df.columns):
            raise ValueError("Overlay missing geometry columns.")

        overlay_df = overlay_df.assign(
            overlap_bgt=lambda df: df.geometry.area / df.geometry_bgt.area,
            overlap_gisib=lambda df: df.geometry.area / df.geometry_gisib.area,
            geometry_equal=lambda df: df.set_geometry("geometry_gisib").geom_equals_exact(
                df.geometry_bgt, tolerance=self.tolerance
            ),
        )
        return overlay_df

    def filter_overlap_min_ratio(self, overlap_df: GeoDataFrame) -> GeoDataFrame:
        """
        Retain only intersections where at least one overlap value is above the configured minimum,
        and the combined overlap is > 0.5.
        """
        return overlap_df[
            (
                    (overlap_df["overlap_bgt"] >= self.min_ratio) |
                    (overlap_df["overlap_gisib"] >= self.min_ratio)
            ) &
            ((overlap_df["overlap_bgt"] + overlap_df["overlap_gisib"]) > 0.6)
            ].copy()

    def _add_match_flags(self, df: GeoDataFrame) -> GeoDataFrame:
        """
        Annotate with perfect_match and rel_match flags.
        """
        df = df.copy()
        df["perfect_match"] = (df["overlap_bgt"] > 0.90) & (df["overlap_gisib"] > 0.90)
        rel_hoog = df[self.gisib_hoogteligging_col].fillna(0).astype(int)
        hoogtelig = df[self.bgt_hoogteligging_col].fillna(0).astype(int)
        df["rel_match"] = rel_hoog == hoogtelig
        return df

    def get_perfect_rel_matches(self, filtered_df: GeoDataFrame) -> GeoDataFrame:
        """
        Returns a GeoDataFrame with rows where both perfect_match and rel_match hold.
        """
        df = self._add_match_flags(filtered_df)
        perfect_rel_mask = df["perfect_match"] & df["rel_match"]
        guids_with_perfect_rel = set(df.loc[perfect_rel_mask, self.gisib_id_col].unique())

        subset1 = df[perfect_rel_mask & df[self.gisib_id_col].isin(guids_with_perfect_rel)]
        subset2 = df[~df[self.gisib_id_col].isin(guids_with_perfect_rel)]
        merged_df = gpd.GeoDataFrame(pd.concat([subset1, subset2]), crs=df.crs)

        guids_multi_rel = (
            merged_df.groupby(self.gisib_id_col)["rel_match"]
            .nunique()
            .reset_index()
            .query("rel_match == 2")[self.gisib_id_col]
            .tolist()
        )
        subset3 = merged_df[merged_df[self.gisib_id_col].isin(guids_multi_rel) & merged_df["perfect_match"]]
        rest = merged_df[~merged_df[self.gisib_id_col].isin(subset3[self.gisib_id_col])]


        return gpd.GeoDataFrame(pd.concat([subset3, rest]), crs=df.crs)

    def add_intersection_counts(self, overlaps: GeoDataFrame) -> GeoDataFrame:
        """
        Annotate intersection GeoDataFrame with 1:many/many:1 relationship counts.
        """
        guids_per_lokaalid = (
            overlaps.groupby(self.bgt_id_col)[self.gisib_id_col].nunique().rename("guids_per_lokaalid")
        )
        lokaalids_per_guid = (
            overlaps.groupby(self.gisib_id_col)[self.bgt_id_col].nunique().rename("lokaalids_per_guid")
        )

        overlaps = overlaps.merge(
            guids_per_lokaalid, on=self.bgt_id_col, how="left"
        ).merge(
            lokaalids_per_guid, on=self.gisib_id_col, how="left"
        )
        return overlaps

    def preprocess(self) -> GeoDataFrame:
        """
        Pipeline: overlay, min_ratio filtering, intersection counts, and perfect/rel matching.
        Returns:
            Final GeoDataFrame for downstream bucketing.
        """
        overlay = self.calculate_overlap_df()
        filtered = self.filter_overlap_min_ratio(overlay)
        with_counts = self.add_intersection_counts(filtered)
        return self.get_perfect_rel_matches(with_counts)

    def no_matches(self, intersection_df: GeoDataFrame) -> GeoDataFrame:
        """
        Returns GISIB rows that do not have any spatial intersection with BGT.
        """
        matched = set(intersection_df[self.gisib_id_col])
        return self.gisib[~self.gisib[self.gisib_id_col].isin(matched)].copy()

    def select_1_to_1_geometric_matches(
        self, intersection_df: GeoDataFrame
    ) -> Tuple[GeoDataFrame, GeoDataFrame]:
        """
        Returns strict 1:1 geometric matches and all remaining.
        """
        is_1_to_1 = (intersection_df["guids_per_lokaalid"] == 1) & (
            intersection_df["lokaalids_per_guid"] == 1
        )
        is_geom = (intersection_df["overlap_bgt"] > 0.75) & (
            intersection_df["overlap_gisib"] > 0.75
        )
        matches = intersection_df[is_1_to_1 & is_geom].copy()
        remaining = intersection_df[~(is_1_to_1 & is_geom)].copy()
        return matches, remaining

    def build_bgt_gisib_grouped(self, intersection_df: GeoDataFrame, tolerance: float = 0.01) -> GeoDataFrame:
        """
        Aggregates all GISIB geometries per BGT object, with summary statistics for 1 BGT : N GISIB.
        """
        df = intersection_df.fillna({self.gisib_hoogteligging_col: 0}).copy()
        df[self.gisib_hoogteligging_col] = df[self.gisib_hoogteligging_col].astype(int)
        agg = (
            df.groupby([self.bgt_id_col, self.gisib_hoogteligging_col])
            .agg(
                geometry_gisib=("geometry_gisib", unary_union),
                overlap_bgt=("overlap_bgt", "sum"),
                geometry_bgt=("geometry_bgt", "first"),
                overlap_gisib=("overlap_gisib", "mean"),
                guid_list=(self.gisib_id_col, list),
                guid_count=(self.gisib_id_col, "count"),
                ONDERHOUDSPLICHTIGE=("ONDERHOUDSPLICHTIGE", lambda x: x.nunique(dropna=False)),
                TYPE=("TYPE", lambda x: x.nunique(dropna=False)),
                TYPE_GEDETAILLEERD=("TYPE_GEDETAILLEERD", lambda x: x.nunique(dropna=False)),
                BEHEERDER_GEDETAILLEERD=("BEHEERDER_GEDETAILLEERD", lambda x: x.nunique(dropna=False)),
                BUURT=("BUURT", "first"),
            ).reset_index()
        )
        gdf = gpd.GeoDataFrame(agg, geometry="geometry_gisib", crs=intersection_df.crs)
        gdf["geometry_equal"] = gdf.set_geometry("geometry_gisib").geom_equals_exact(
            gdf["geometry_bgt"].set_crs(intersection_df.crs), tolerance=tolerance
        )
        gdf["overlap_gisib10"] = (gdf["overlap_gisib"] > 0.90) & (gdf["overlap_gisib"] < 1.1)
        gdf["overlap_bgt10"] = (gdf["overlap_bgt"] > 0.90) & (gdf["overlap_bgt"] < 1.1)
        gdf["guid_count"] = gdf["guid_list"].apply(len)
        return gdf

    def build_gisib_bgt_grouped(self, intersection_df: GeoDataFrame, tolerance: float = 0.01) -> GeoDataFrame:
        """
        Aggregates all BGT geometries per GISIB object, with summary statistics for 1 GISIB : N BGT.
        """
        df = intersection_df.fillna({self.bgt_hoogteligging_col: 0}).copy()
        df[self.bgt_hoogteligging_col] = df[self.bgt_hoogteligging_col].astype(int)
        agg = (
            df.groupby([self.gisib_id_col, self.bgt_hoogteligging_col])
            .agg(
                geometry_bgt=("geometry_bgt", unary_union),
                overlap_gisib=("overlap_gisib", "sum"),
                geometry_gisib=("geometry_gisib", "first"),
                lokaalid_list=(self.bgt_id_col, list),
                lokaalid_count=(self.bgt_id_col, "count"),
                overlap_bgt=("overlap_bgt", "mean"),
            ).reset_index()
        )
        gdf = gpd.GeoDataFrame(agg, geometry="geometry_gisib", crs=intersection_df.crs)
        gdf["geometry_equal"] = gdf.set_geometry("geometry_gisib").geom_equals_exact(
            gdf["geometry_bgt"].set_crs(intersection_df.crs), tolerance=tolerance
        )
        gdf["overlap_gisib10"] = (gdf["overlap_gisib"] > 0.90) & (gdf["overlap_gisib"] < 1.1)
        gdf["overlap_bgt10"] = (gdf["overlap_bgt"] > 0.90) & (gdf["overlap_bgt"] < 1.1)
        gdf["lokaalid_count"] = gdf["lokaalid_list"].apply(len)
        return gdf

    def _select_1_bgt_to_n_gisib_overlap5_split(self, bgt_gisib: GeoDataFrame) -> Set:
        """
        Return set of guids in BGT : N GISIB groupings that should be split (all overlap ≈ 1).
        """
        mask = (
            (bgt_gisib["guid_count"] > 1)
            & bgt_gisib["overlap_bgt10"]
            & bgt_gisib["overlap_gisib10"]
        )
        return set(bgt_gisib.loc[mask, "guid_list"].explode())

    def _select_1_bgt_to_n_gisib_overlap5_merge(self, bgt_gisib: GeoDataFrame) -> Set:
        """
        Return set of guids in BGT : N GISIB groupings that can be merged, based on attribute conditions.
        """
        mask = (
            (bgt_gisib["guid_count"] > 1)
            & bgt_gisib["overlap_bgt10"]
            & bgt_gisib["overlap_gisib10"]
            & (bgt_gisib["ONDERHOUDSPLICHTIGE"] == 1)
            & (bgt_gisib["TYPE_GEDETAILLEERD"] == 1)
            & (bgt_gisib["BEHEERDER_GEDETAILLEERD"] < 2)
        )
        return set(bgt_gisib.loc[mask, "guid_list"].explode())

    def select_1_bgt_to_n_gisib_overlap5_matches(
        self, intersection_df: GeoDataFrame
    ) -> Tuple[GeoDataFrame, GeoDataFrame, GeoDataFrame]:
        """
        Partition intersection_df into:
            - bucket_merge: for guids to merge,
            - bucket_split: for guids to split,
            - remaining: all other rows.
        """
        bgt_gisib = self.build_bgt_gisib_grouped(intersection_df)
        guids_to_merge = self._select_1_bgt_to_n_gisib_overlap5_merge(bgt_gisib)
        guids_to_split = self._select_1_bgt_to_n_gisib_overlap5_split(bgt_gisib) - guids_to_merge
        bucket_merge = intersection_df[intersection_df[self.gisib_id_col].isin(guids_to_merge)]
        bucket_split = intersection_df[intersection_df[self.gisib_id_col].isin(guids_to_split)]
        remaining = intersection_df[
            ~intersection_df[self.gisib_id_col].isin(guids_to_merge | guids_to_split)
        ]
        return bucket_merge, bucket_split, remaining

    def select_1_gisib_to_n_bgt_overlap5_matches(
        self, intersection_df: GeoDataFrame
    ) -> Tuple[GeoDataFrame, GeoDataFrame]:
        """
        Partition for GISIB objects that overlap ≈ 100% with >1 BGT.
        """
        gisib_bgt = self.build_gisib_bgt_grouped(intersection_df)
        mask = (gisib_bgt["overlap_bgt10"]) & (gisib_bgt["overlap_gisib10"]) & (gisib_bgt["lokaalid_count"] > 1)
        guids_to_split = set(gisib_bgt.loc[mask, self.gisib_id_col])
        bucket_split = intersection_df[intersection_df[self.gisib_id_col].isin(guids_to_split)]
        remaining = intersection_df[~intersection_df[self.gisib_id_col].isin(guids_to_split)]
        return bucket_split, remaining

    def geom_match(self, intersection_df: GeoDataFrame) -> Tuple[GeoDataFrame, GeoDataFrame]:
        """
        Returns matches where both overlap_bgt and overlap_gisib > 0.75, and remaining.
        """
        mask = ((intersection_df["overlap_bgt"] > 0.75) & (intersection_df["overlap_gisib"] > 0.75))
        geom_matches = intersection_df[mask]
        # assert every gisib_id is unique in matches
        if not geom_matches.empty:
            # als er een assert function hier zit is het waarschijnlijk een mismatch met de hagen?
            # Voer de volgende print statements, check in welke laag deze zit, als het groenobjecten is, moet je in de matcher_Groenobjecten
            # het matchingspercentage van de hagen eventueel aanpassen. Laat zien wat het huidige percentage is in de onderstaande statements
        #   # vul zelf de guid even in.

            if geom_matches[self.gisib_id_col].value_counts().max() > 1:
                dup_counts = geom_matches[self.gisib_id_col].value_counts()
                duplicates = geom_matches.loc[geom_matches[self.gisib_id_col].isin(dup_counts[dup_counts > 1].index),[self.gisib_id_col,self.bgt_id_col,"OBJECTTYPE","TYPE","ObjectType"]]
                print(duplicates)
                print(duplicates.loc[:,self.gisib_id_col].unique())
            assert geom_matches[self.gisib_id_col].value_counts().max() == 1
        remaining = intersection_df[~intersection_df[self.gisib_id_col].isin(geom_matches[self.gisib_id_col].unique())]

        return geom_matches, remaining

    def clip_match(self, intersection_df: GeoDataFrame) -> Tuple[GeoDataFrame, GeoDataFrame]:
        """
        Returns 'clip' matches, where overlap_bgt < 50 and overlap_gisib > 0.85,
        sorted by height equality, one per guid. Remaining returned as well.
        """
        df = (
            intersection_df.loc[
                lambda df: ((df["overlap_bgt"] < 0.50) & (df["overlap_gisib"] > 0.85))
            ]
            .fillna({self.gisib_hoogteligging_col: 0})
            .assign(
                hoogte_equal=lambda df: df[self.bgt_hoogteligging_col].astype(int)
                == df[self.gisib_hoogteligging_col].astype(int)
            )
            .sort_values("hoogte_equal", ascending=False)
            .drop_duplicates(self.gisib_id_col, keep="first")
            .drop(columns="hoogte_equal")
        )
        matched_ids = set(df[self.gisib_id_col])
        remaining = intersection_df[~intersection_df[self.gisib_id_col].isin(matched_ids)]
        return df, remaining

    def unique_assets(self) -> int:
        """
        Returns unique asset count in GISIB.
        """
        return self.gisib[self.gisib_id_col].nunique()
    def run(self) -> Dict[str,GeoDataFrame]:
        """
        Runs the full bucketing/matching pipeline and returns a list of buckets.
        """
        intersection_df = self.preprocess()
        logger.info(f"{self.__class__.__name__} starts writing buckets")
        logger.info("Preprocessing complete, intersection rows: %d", len(intersection_df))
        logger.info("Number of unique gisib objects: %d",intersection_df.loc[:,self.gisib_id_col].nunique())

        # gisib object has no BGT match: or add object in BGT or remove gisib
        bucket0 = self.no_matches(intersection_df)
        logger.info("No matches: %d", len(bucket0))


        # bucket1 : gisib and bgt matches:
        bucket1, remaining = self.select_1_to_1_geometric_matches(intersection_df)
        logger.info("1:1 geometric matches: %d, remaining: %d", len(bucket1), remaining.loc[:,self.gisib_id_col].nunique())
        print(remaining.loc[lambda df: df.GUID == "{4537BC0E-1739-4B27-B8D7-44C82948A4E0}"])
        print(1)

        # bucket 2 : gisib objects to merge - so 1 will get the id of BGT and the other one deleted
        # bucket 3 : gisib objects to split: so BGT needs to be splitted
        bucket2, bucket3, remaining = self.select_1_bgt_to_n_gisib_overlap5_matches(remaining)
        logger.info("1 BGT : N GISIB merge: %d, split: %d, remaining: %d", len(bucket2), len(bucket3), remaining.loc[:,self.gisib_id_col].nunique())
        print(remaining.loc[lambda df: df.GUID == "{4537BC0E-1739-4B27-B8D7-44C82948A4E0}"])
        print(2)
        # bucket 4: gisib object that needs to be adjusted because BGT is splitted
        bucket4, remaining = self.select_1_gisib_to_n_bgt_overlap5_matches(remaining)
        logger.info("1 GISIB : N BGT split: %d, remaining: %d", len(bucket4), remaining.loc[:,self.gisib_id_col].nunique())
        print(remaining.loc[lambda df: df.GUID == "{4537BC0E-1739-4B27-B8D7-44C82948A4E0}"])
        print(3)
        # bucket 5:  geometric match, only guid and identificatie lokaal id
        bucket5, remaining = self.geom_match(remaining)
        logger.info("Geom 75%% match: %d, remaining: %d", len(bucket5), remaining.loc[:,self.gisib_id_col].nunique())
        print(remaining.loc[lambda df: df.GUID == "{4537BC0E-1739-4B27-B8D7-44C82948A4E0}"])
        print(4)
        # clips: or process them in BGT, or delete them from gisib
        bucket6, remaining = self.clip_match(remaining)
        logger.info("Clip matches: %d, remaining: %d", len(bucket6), remaining.loc[:,self.gisib_id_col].nunique())
        print(remaining.loc[lambda df: df.GUID == "{4537BC0E-1739-4B27-B8D7-44C82948A4E0}"])
        print(5)



        # seperate by asset:
        # gisib/bgt - description
        # bucket0: gisib objects with no match
        # bucket0: only gisib objects
        # bucket1: 1-to-1 geom matches
        # bucket1: both gisib and bgt objects
        # bucket2: 1 BGT - N GISIB - merge gisib to 1 object?
        # bucket2: both gisib and bgt objects
        # bucket3: 1 BGT - N GISIB - split BGT
        # bucket3: both gisib and bgt objects
        # bucket4: 1 GISIB - N BGT - split gisib
        # bucket4: both gisib and bgt objects
        # bucket5: 75% overlap matches - match
        # bucket5: both gisib and bgt objects
        # bucket6: clip matches - smaller gisib object in larger BGT objects
        # bucket6: both gisib and bgt objects
        # remaining:
        # remaining: both gisib and bgt objects

        result = {
            BucketsBase.BUCKET0.value: bucket0,
            BucketsBase.BUCKET1.value: bucket1,
            BucketsBase.BUCKET2.value: bucket2,
            BucketsBase.BUCKET3.value: bucket3,
            BucketsBase.BUCKET4.value: bucket4,
            BucketsBase.BUCKET5.value: bucket5,
            BucketsBase.BUCKET6.value: bucket6,
            BucketsBase.REMAINING.value: remaining
        }
        x = result[BucketsBase.BUCKET2.value]
        print("What is the bucketname")
        print(BucketsBase.BUCKET2.value)

        return result

    def prepare_imports(self):
        results = self.run()


df = pd.read_excel("path_naar_excel.xlsx")
