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

    # ---- Filter assets ----
    filtered_assets = []
    first_crs = None

    for key, gdf in assets.items():
        if not isinstance(gdf, gpd.GeoDataFrame):
            continue
        if first_crs is None:
            first_crs = gdf.crs
        if gisib_id_column not in gdf.columns:
            continue

        sub = gdf[gdf[gisib_id_column].isin(guid_set)].copy()
        if not sub.empty:
            sub["asset_key"] = key
            filtered_assets.append(sub)

    if filtered_assets:
        assets_invalid = gpd.GeoDataFrame(
            pd.concat(filtered_assets, ignore_index=True),
            geometry=filtered_assets[0].geometry.name,
            crs=filtered_assets[0].crs,
        )
    else:
        assets_invalid = gpd.GeoDataFrame(geometry=[], crs=first_crs)

    # ---- Exclude skip types (optional) ----
    if skip_types and not assets_invalid.empty and skip_types_column in assets_invalid.columns:
        assets_invalid = assets_invalid[~assets_invalid[skip_types_column].isin(skip_types)].copy()

    # ---- Filter BGT ----
    if bgt_id_column not in bgt.columns:
        raise KeyError(f"'{bgt_id_column}' not found in BGT")

    bgt_invalid = bgt[bgt[bgt_id_column].isin(lokaalids_set)].copy()

    # ---- Write both layers ----
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        assets_invalid.to_file(output_path, layer=gisib_layer, driver="GPKG")
        bgt_invalid.to_file(output_path, layer=bgt_layer, driver="GPKG")

    return assets_invalid, bgt_invalid
