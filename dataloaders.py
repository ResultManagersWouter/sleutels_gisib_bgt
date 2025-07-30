import os
from enum import Enum
import pandas as pd
from enums import AssetType
from columns_config import column_mappings
from typing import Optional, Union, List, Tuple
import geopandas as gpd
import logging
import fiona

logger = logging.getLogger(__name__)


def _read_bgt_shapes(folder: str, columns: list[str]):
    """
    Reads shapefiles from a given folder and returns a GeoDataFrame containing specified columns.

    Parameters:
    ----------
    folder : str
        The path to the folder containing shapefiles (.shp).
    columns : List[str]
        A list of column names to retain from each shapefile.
    objecttypes : List[str]
        A list of object types to filter (optional, currently unused).

    Returns:
    -------
    gpd.GeoDataFrame
        A concatenated GeoDataFrame containing the specified columns from all shapefiles in the folder.
    """
    shapefiles = [f for f in os.listdir(folder) if f.endswith(".shp")]
    gdfs = []

    for shp in shapefiles:
        path = os.path.join(folder, shp)
        gdf = gpd.read_file(path).rename(columns={"FysiekVplu": "FysiekVPlu"})
        gdfs.append(gdf[columns])

    return pd.concat(gdfs, ignore_index=True)


def read_bgt_shapes(
    folder: str, columns: List[str], objecttypes: List[str], object_col: str
) -> gpd.GeoDataFrame:
    """
    Reads shapefiles from a specified folder and returns a concatenated GeoDataFrame containing only the specified columns.

    Parameters:
    ----------
    folder : str
        The path to the folder containing shapefiles (.shp files).
    columns : List[str]
        A list of column names to retain from each shapefile.
    objecttypes : List[str]
        A list of BGT object types to filter for. Only the following values are allowed, as defined in the ControleTabelBGT enum:

            - 'Wegdeel'
            - 'Ondersteunend wegdeel'
            - 'Begroeid terreindeel'
            - 'Onbegroeid terreindeel'
            - 'Ondersteunend waterdeel'
            - 'Vegetatieobject'
            - 'Weginrichtingselement'

        These object types correspond to three high-level groups used in validation:
        - Groenobject
        - Terreindeel
        - Verhardingsobject

        **Note:** Filtering based on these types must be implemented within the function; currently, this parameter is accepted but not applied.

    Returns:
    -------
    gpd.GeoDataFrame
        A single GeoDataFrame containing the specified columns from all shapefiles in the folder.

    Notes:
    -----
    - Only files ending in `.shp` are processed.
    - Assumes all shapefiles contain the requested columns.
    - Assumes uniform schema and compatible CRS across shapefiles.
    - `objecttypes` filtering is declared but not currently active in the function logic.
    """
    gdfs = _read_bgt_shapes(folder=folder, columns=columns)
    return gdfs.loc[lambda df: df.loc[:, object_col].isin(objecttypes)]


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
        .loc[lambda df: df.lokaalid.notnull()]  # Keep only rows with a lokaalid
    )
    return gdf, AssetType  # adjust as needed


def read_gisib(
    fp_gisib: str,
    layer: Optional[Union[str, AssetType]] = None,
    columns: Optional[List[str]] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
) -> gpd.GeoDataFrame:
    """
    Reads a GISIB dataset layer using pyogrio and returns a cleaned GeoDataFrame.

    Args:
        fp_gisib (str): Path to the GISIB GPKG file.
        layer (Optional[str or AssetType]): Layer name to read. If None, reads the first layer.
        columns (Optional[List[str]]): List of columns to load. If None, all columns are loaded.
        bbox (Optional[Tuple[float, float, float, float]]): Optional bounding box (minx, miny, maxx, maxy)
            to spatially filter the dataset.

    Returns:
        gpd.GeoDataFrame: Cleaned GISIB data with fixed geometries.
    """
    # Determine layer name
    if layer is None:
        layer_name = fiona.listlayers(fp_gisib)[0]
    elif isinstance(layer, AssetType):
        available_layers = fiona.listlayers(fp_gisib)
        if layer.value in available_layers:
            layer_name = layer.value
        else:
            layer_name = available_layers[0]
            logging.info(
                f"Layer name is different than input:\n Given = {layer}\n Layer = {available_layers[0]}"
            )

    else:
        layer_name = layer

    # Check casing of first column to determine column mode
    first_column = gpd.read_file(
        fp_gisib, layer=layer_name, engine="pyogrio", rows=1
    ).columns[0]
    mapper = column_mappings.get(layer, {})

    # Case 1: uppercase → map to lowercase before reading
    if first_column == "id":
        mapper = column_mappings.get(layer, {})  # lower → UPPER
        gdf = gpd.read_file(
            fp_gisib,
            layer=layer_name,
            engine="pyogrio",
            columns=columns,
            bbox=bbox,
        )
        gdf = gdf.rename(columns=mapper)

    # Case 2: lowercase → read and map after reading
    elif first_column == "ID":
        mapped_columns = [mapper.get(col) for col in columns]
        gdf = gpd.read_file(
            fp_gisib,
            layer=layer_name,
            engine="pyogrio",
            columns=mapped_columns,
            bbox=bbox,
        )
        # print(gdf.head())
    return gdf.assign(geometry=lambda df: df.geometry.buffer(0))


def read_controle_tabel(
    filepath: str, columns: List[str], filterEnum: Enum, filter_col: str
) -> pd.DataFrame:
    """
    Reads and filters an Excel control table based on a specific column and Enum filter.

    Parameters:
    ----------
    filepath : str
        Path to the Excel file to read.

    columns : List[str]
        List of column names to select from the Excel file.

    filterEnum : Enum
        Enum class containing the allowed values to filter the DataFrame on.

    filter_col : str
        Name of the column in the Excel file to filter using the values from `filterEnum`.

    Returns:
    -------
    pd.DataFrame
        A pandas DataFrame containing only the specified columns and rows
        where `filter_col` matches one of the values in `filterEnum`.

    Example:
    -------
    >>> from enum import Enum
    >>> class Status(Enum):
    ...     ACTIVE = 'active'
    ...     INACTIVE = 'inactive'
    >>> read_controle_tabel('data.xlsx', ['id', 'status'], Status, 'status')
    """
    enum_values = [e.value for e in filterEnum]
    df = pd.read_excel(filepath).loc[
        lambda df: df[filter_col].isin(enum_values), columns
    ]
    return df
