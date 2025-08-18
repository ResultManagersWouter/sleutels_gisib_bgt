import geopandas as gpd
import pandas as pd


def filter_hagen(intersection_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    hagen_intersection_df = (
        intersection_df.loc[lambda df: df.TYPE.isin(["Haag"])]
        .loc[lambda df: df.overlap_gisib > 0.9]
        .loc[lambda df: df.ObjectType == "Vegetatieobject"]
    )

    # exclude these guids from the original df
    intersection_df = intersection_df.loc[lambda df: ~df.GUID.isin(hagen_intersection_df.GUID.tolist())]
    # re-add the filtered hagen rows at the end
    intersection_df = pd.concat([intersection_df, hagen_intersection_df], ignore_index=True)

    return intersection_df


