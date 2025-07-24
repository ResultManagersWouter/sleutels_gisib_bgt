from typing import Optional, Tuple, List, Set, Union
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely import unary_union
import pandas as pd
import logging

from .matcher_base import MatcherBase
logger = logging.getLogger(__name__)

class GroenobjectenMatcher(MatcherBase):
    pass