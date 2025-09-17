import os
from typing import Dict, List, Tuple, Optional
import warnings
import geopandas as gpd
import pandas as pd


def write_invalid_types_to_geodataframe(
    assets: Dict[str, gpd.GeoDataFrame],
    bgt: gpd.GeoDataFrame,
    invalid_type_combinations: List[dict],
    *,
    gisib_id_column: str,
    bgt_id_column: str,
    skip_types: Optional[List[str]] = None,
    skip_types_column: str = "TYPE",
    output_path: Optional[str] = None,
    gisib_layer: str = "gisib",
    bgt_layer: str = "bgt",
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Filter:
      - assets by GUIDs from 'guid'
      - bgt by lokaal IDs from 'lokaalid'

    If output_path is provided:
      - writes assets to layer `gisib_layer`
      - writes bgt to layer `bgt_layer`
    """

    # ---- Collect GUIDs and lokaalids ----
    guid_set = {r["guid"] for r in invalid_type_combinations if "guid" in r}
    lokaalids_set = {r["lokaalid"] for r in invalid_type_combinations if "lokaalid" in r}

    if not guid_set:
        warnings.warn("No GUIDs found; assets result will be empty.")
    if not lokaalids_set:
        warnings.warn("No lokaalids found; BGT result will be empty.")

    invalid_gisib_ = pd.concat(list(assets.values())).loc[lambda df: df.loc[:,gisib_id_column].isin(list(guid_set))]

    # ---- Exclude skip types (optional) ----
    if skip_types and not invalid_gisib_.empty and skip_types_column in invalid_gisib_.columns:
        invalid_gisib_ = invalid_gisib_[~invalid_gisib_[skip_types_column].isin(skip_types)].copy()

    # ---- Filter BGT ----
    if bgt_id_column not in bgt.columns:
        raise KeyError(f"'{bgt_id_column}' not found in BGT")

    bgt_invalid = bgt.loc[lambda df: df.loc[:,bgt_id_column].isin(list(lokaalids_set))].copy()
    print(bgt_invalid.shape)
    print(invalid_gisib_.shape)
    # ---- Write both layers ----
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        invalid_gisib_.to_file(output_path, layer=gisib_layer, driver="GPKG")
        bgt_invalid.to_file(output_path, layer=bgt_layer, driver="GPKG")

    return invalid_gisib_, bgt_invalid
