from typing import List, Optional, Union
import geopandas as gpd
import pyogrio
from asset_config import AssetType
from columns_config import column_mappings

def read_bgt(fp_bgt: str, columns: List[str]) -> gpd.GeoDataFrame:
    """
    Reads a BGT dataset and returns a cleaned GeoDataFrame.

    Args:
        fp_bgt (str): Path to the BGT file.
        columns (list): List of columns to load.

    Returns:
        gpd.GeoDataFrame: Cleaned and filtered BGT data.
    """
    gdf = (
        gpd.read_file(fp_bgt, columns=columns)
        .assign(geometry=lambda df: df.geometry.buffer(0))  # Clean invalid geometries
        .loc[lambda df: df.lokaalid.notnull()]              # Keep only rows with a lokaalid
    )
    return gdf

def read_gisib(
    fp_gisib: str,
    layer: Optional[Union[str, AssetType]] = None,
    columns: Optional[List[str]] = None
) -> gpd.GeoDataFrame:
    """
    Reads a GISIB dataset layer using pyogrio and returns a cleaned GeoDataFrame.

    Args:
        fp_gisib (str): Path to the GISIB GPKG file.
        layer (Optional[str or AssetType]): Layer name to read. If None, reads the first layer.
        columns (Optional[List[str]]): List of columns to load. If None, all columns are loaded.

    Returns:
        gpd.GeoDataFrame: Cleaned GISIB data with fixed geometries.
    """
    # Determine layer name
    if layer is None:
        layer_name = pyogrio.list_layers(fp_gisib)[0]
    elif isinstance(layer, AssetType):
        layer_name = layer.value
    else:
        layer_name = layer

    # Check casing of first column to determine column mode
    first_column = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", rows=1).columns[0]

    # Case 1: uppercase → map to lowercase before reading
    if first_column == "ID":
        mapper = column_mappings.get(layer, {})
        reverse_mapper = {v: k for k, v in mapper.items()}  # UPPER → lower
        if columns:
            mapped_columns = [reverse_mapper.get(col, col) for col in columns]
        else:
            mapped_columns = None
        gdf = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", columns=mapped_columns)
        gdf = gdf.rename(columns=mapper)

    # Case 2: lowercase → read and map after reading
    else:
        gdf = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", columns=columns)
        gdf = gdf.rename(columns=column_mappings.get(layer, {}))

    return gdf.assign(geometry=lambda df: df.geometry.buffer(0))

