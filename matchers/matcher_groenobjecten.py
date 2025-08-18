from typing import Optional, Tuple, List, Set, Union
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely import unary_union
import pandas as pd
import logging

from .matcher_base import MatcherBase
from .filter_matches import filter_hagen
logger = logging.getLogger(__name__)

class GroenobjectenMatcher(MatcherBase):
    def preprocess(self) -> GeoDataFrame:
        """
        Pipeline: overlay, min_ratio filtering, intersection counts, and perfect/rel matching.
        Returns:
            Final GeoDataFrame for downstream bucketing.
        """
        overlay = self.calculate_overlap_df()
        filtered = self.filter_overlap_min_ratio(overlay)
        filtered_hagen = filter_hagen(intersection_df=filtered)
        with_counts = self.add_intersection_counts(filtered_hagen)
        print(with_counts.loc[lambda df: df.GUID == "{AFD535EA-EBED-4761-8D7F-188E208A65A2}"])
        return self.get_perfect_rel_matches(with_counts)