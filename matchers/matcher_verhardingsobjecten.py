from typing import Dict
from geopandas import GeoDataFrame
import logging
from buckets import BucketsVRH

from .matcher_base import MatcherBase

logger = logging.getLogger(__name__)


class VerhardingenMatcher(MatcherBase):

    def additional_matches_based_on_overlap(self, intersection_df: GeoDataFrame):
        bucket = (
            intersection_df.assign(
                total_sum=lambda df: df.overlap_gisib + df.overlap_bgt
            )
            .sort_values(by="total_sum")
            .loc[lambda df: df.total_sum > 1.5]
            .drop(columns=["total_sum"])
        )
        remaining = intersection_df[
            ~intersection_df[self.gisib_id_col].isin(bucket.loc[:,self.gisib_id_col].unique().tolist())
        ]
        return bucket, remaining

    def run(self) -> Dict[str, GeoDataFrame]:
        """
        Runs the full bucketing/matching pipeline and returns a dict of named buckets.
        """
        intersection_df = self.preprocess()
        logger.info(f"{self.__class__.__name__} starts writing buckets")
        logger.info(
            "Preprocessing complete, intersection rows: %d", len(intersection_df)
        )

        bucket0 = self.no_matches(intersection_df)
        logger.info("No matches: %d", len(bucket0))

        bucket1, remaining = self.select_1_to_1_geometric_matches(intersection_df)
        logger.info(
            "1:1 geometric matches: %d, remaining: %d", len(bucket1), remaining.loc[:, self.gisib_id_col].nunique()
        )

        bucket2, bucket3, remaining = self.select_1_bgt_to_n_gisib_overlap5_matches(
            remaining
        )
        logger.info(
            "1 BGT : N GISIB merge: %d, split: %d, remaining: %d",
            len(bucket2),
            len(bucket3),
            remaining.loc[:, self.gisib_id_col].nunique(),
        )

        bucket4, remaining = self.select_1_gisib_to_n_bgt_overlap5_matches(remaining)
        logger.info(
            "1 GISIB : N BGT split: %d, remaining: %d", len(bucket4), remaining.loc[:, self.gisib_id_col].nunique()
        )

        bucket5, remaining = self.geom_match(remaining)
        logger.info("Geom 75%% match: %d, remaining: %d", len(bucket5), remaining.loc[:, self.gisib_id_col].nunique())

        bucket6, remaining = self.additional_matches_based_on_overlap(remaining)
        logger.info("Additional matches total sum 150%% match: %d, remaining: %d", len(bucket6),
                    remaining.loc[:, self.gisib_id_col].nunique())

        bucket7, remaining = self.clip_match(remaining)
        logger.info("Clip matches: %d, remaining: %d", len(bucket7), remaining.loc[:, self.gisib_id_col].nunique())

        return {
            BucketsVRH.BUCKET0.value: bucket0,
            BucketsVRH.BUCKET1.value: bucket1,
            BucketsVRH.BUCKET2.value: bucket2,
            BucketsVRH.BUCKET3.value: bucket3,
            BucketsVRH.BUCKET4.value: bucket4,
            BucketsVRH.BUCKET5.value: bucket5,
            BucketsVRH.BUCKET6.value: bucket6,
            BucketsVRH.BUCKET7.value: bucket7,
            BucketsVRH.REMAINING.value: remaining,
        }
