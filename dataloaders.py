import os
from enum import Enum
import pandas as pd
from enums import AssetType
from columns_config import column_mappings
from columns_config import ASSET_SCHEMAS
import geopandas as gpd
import fiona
import logging
from typing import Optional, Union, List, Tuple,Dict
from shapely.geometry import Polygon
from shapely import make_valid


logger = logging.getLogger(__name__)


def _read_bgt_shapes(folder: str, columns: list[str],bbox):
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
        gdf = gpd.read_file(path,bbox=bbox)
        gdfs.append(gdf[columns])
    return pd.concat(gdfs, ignore_index=True)


def read_bgt_shapes(
    folder: str, columns: List[str], objecttypes: List[str], object_col: str,bbox
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
    gdfs = _read_bgt_shapes(folder=folder, columns=columns,bbox=bbox).loc[lambda df: df.geometry.type == "Polygon"]
    # Set CRS to EPSG:28992
    print(gdfs.ObjectType.value_counts())
    print(gdfs.shape)
    gdfs["geometry"] = gdfs.geometry.apply(make_valid)
    print(gdfs.shape)
    gdfs = gdfs.set_crs(28992, allow_override=True)
    print(gdfs.shape)
    x = gdfs.loc[lambda df: df.loc[:, object_col].isin(objecttypes)]
    print(x.shape)
    print(objecttypes)
    return gdfs.loc[lambda df: df.loc[:, object_col].isin(objecttypes)]

def read_gisib(
    fp_gisib: str,
    layer: Optional[Union[str, 'AssetType']] = None,
    columns: Optional[List[str]] = None,
    bbox: Optional[Union[Tuple[float, float, float, float], Polygon]] = None,
    filter_column: Optional[str] = None,
    filter_value: Optional[Union[str, float, int]] = None,
) -> gpd.GeoDataFrame:
    """
    Reads a GISIB dataset layer using pyogrio and returns a cleaned GeoDataFrame.

    Args:
        fp_gisib (str): Path to the GISIB GPKG file.
        layer (Optional[str or AssetType]): Layer name to read. If None, reads the first layer.
        columns (Optional[List[str]]): List of columns to load. If None, all columns are loaded.
        bbox (Optional[Tuple[float, float, float, float]] or Polygon): Spatial filter.
        filter_column (Optional[str]): Name of the column to filter on.
        filter_value (Optional[Union[str, float, int]]): Value to filter on.
            Rows where the column == value OR is null will be retained.

    Returns:
        gpd.GeoDataFrame: Cleaned GISIB data with fixed geometries and EPSG:28992.
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

    # Convert Polygon to bounding box for fast read
    filter_polygon = None
    if isinstance(bbox, Polygon):
        filter_polygon = bbox
        bbox = bbox.bounds

    # Determine casing and mapping
    first_column = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", rows=1).columns[0]
    mapper = column_mappings.get(layer, {})

    if first_column == "id":
        gdf = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", columns=columns, bbox=bbox)
        gdf = gdf.rename(columns=mapper)
    elif first_column == "ID":
        mapped_columns = [mapper.get(col) for col in columns] if columns else None
        gdf = gpd.read_file(fp_gisib, layer=layer_name, engine="pyogrio", columns=mapped_columns, bbox=bbox)

    # Geometry filter
    if filter_polygon is not None:
        gdf = gdf[gdf.geometry.intersects(filter_polygon)]

    # Column filter: keep rows where column == value OR is null
    if filter_column is not None and filter_value is not None:
        gdf = gdf[(gdf[filter_column] == filter_value) | (gdf[filter_column].isna())]

    # Set CRS to EPSG:28992
    gdf = gdf.set_crs(28992, allow_override=True)

    return gdf.assign(geometry=lambda df: df.geometry.apply(make_valid).buffer(0))


def read_controle_tabel(
    filepath: str, columns: List[str], filterEnum: Enum, filter_col: str,mapping : dict,
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
    ].rename(columns=mapping)
    return df

def read_gebied(filepath: str, gebied: str) -> Tuple[float, float, float, float]:
    """
    Reads a GeoDataFrame from the specified file and extracts the bounding box
    for features that match the given 'gebied' value.

    Args:
        filepath (str): Path to the file to read (e.g., a GeoPackage or shapefile).
        gebied (str): The value to match in the 'gebied' column for filtering.

    Returns:
        Tuple[float, float, float, float]: Bounding box (minx, miny, maxx, maxy) of the selected features.

    Raises:
        ValueError: If no features match the specified gebied.
    """
    gdf = gpd.read_file(filepath)

    if gebied not in gdf.naam.to_list():
        raise ValueError("'gebied' column not found in the file.")

    selection = gdf[gdf["naam"] == gebied]

    if selection.empty:
        logging.info(gdf.naam.unique())
        raise ValueError(f"No features found with naam = '{gebied}'")


    return selection.geometry.unary_union


def load_assets(
    bbox: Optional[tuple] = None,
    gebied_col: str = "gebied",
    gebied: Optional[str] = None,
    use_schema_columns: bool = True
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Load all GISIB asset layers filtered by bbox and gebied.

    Args:
        bbox (tuple, optional): Bounding box for spatial filtering (xmin, ymin, xmax, ymax)
        gebied_col (str): Column name used for gebied filtering
        gebied (str, optional): Value to match in the gebied column
        use_schema_columns (bool): If True, loads only columns defined in ASSET_SCHEMAS. If False, loads all columns.

    Returns:
        Dict[str, GeoDataFrame]: Dictionary of asset name -> GeoDataFrame
    """
    return {
        AssetType.TERREINDEEL.value: read_gisib(
            fp_gisib=os.environ.get("FP_TRD"),
            columns=ASSET_SCHEMAS[AssetType.TERREINDEEL] if use_schema_columns else None,
            layer=AssetType.TERREINDEEL,
            bbox=bbox,
            filter_column=gebied_col,
            filter_value=gebied,
        ),
        AssetType.GROENOBJECTEN.value: read_gisib(
            fp_gisib=os.environ.get("FP_GRN"),
            columns=ASSET_SCHEMAS[AssetType.GROENOBJECTEN] if use_schema_columns else None,
            layer=AssetType.GROENOBJECTEN,
            bbox=bbox,
            filter_column=gebied_col,
            filter_value=gebied,
        ),
        AssetType.VERHARDINGEN.value: read_gisib(
            fp_gisib=os.environ.get("FP_VRH"),
            columns=ASSET_SCHEMAS[AssetType.VERHARDINGEN] if use_schema_columns else None,
            layer=AssetType.VERHARDINGEN,
            bbox=bbox,
            filter_column=gebied_col,
            filter_value=gebied,
        ),
    }

