import geopandas as gpd
import pandas as pd
from datetime import date
from itertools import combinations


class GisibValidator:
    def __init__(
        self,
        assets: dict[str, gpd.GeoDataFrame],
        gisib_id_col: str,
        relatieve_hoogteligging_col: str,
        objecttype_col: str,
        threshold: float = 0.5,
        write_outputs: dict[str, bool] = {
            "internal": False,
            "cross": False,
            "all": True,
        },
        gpkg_path=None,
    ):
        """
        Parameters:
            assets: dict of object type to GeoDataFrame.
            write_outputs: If True, writes output files.
                internal: between a single asset - exceluding relatieve hoogteligging
                cross: between two assets - excluding relatieve hoogteligging
                all : internal and cross including relatieve hoogteligging
            gpkg_path: Path for GeoPackage file. Defaults to "overlaps_<today>.gpkg"
        """
        self.assets = assets
        self.standard_crs = "EPSG:28992"
        self.gisib_id_col = gisib_id_col
        self.threshold = threshold
        self.gisib = self._combine_assets()
        self.write_outputs = write_outputs
        self.objecttype_col = objecttype_col
        self.relatieve_hoogteligging_col = relatieve_hoogteligging_col
        if gpkg_path is None:
            today_str = date.today().isoformat()
            gpkg_path = f"overlaps_{today_str}.gpkg"
        self.gpkg_path = gpkg_path

    def _combine_assets(self):
        dfs = []
        for name, gdf in self.assets.items():
            gdf = gdf.copy().to_crs(self.standard_crs)
            gdf["objecttype"] = name
            dfs.append(gdf)
        return pd.concat(dfs).reset_index(drop=True)

    def _filter_significant_overlaps(
        self, df, left_geom_col="geometry_1", right_geom_col="geometry_2"
    ):
        if df.empty:
            return df
        df = df.assign(
            overlap_1=lambda d: d.geometry.area / d[left_geom_col].area,
            overlap_2=lambda d: d.geometry.area / d[right_geom_col].area,
        )
        return df[
            (df["overlap_1"] > self.threshold) & (df["overlap_2"] > self.threshold)
        ]

    def validate_no_internal_overlap(self):
        """
        Checks for internal overlaps within each asset/object type.
        Returns a dict: asset_type -> overlaps_gdf
        """
        results = {}
        for name, gdf in self.assets.items():
            gdf = gdf.copy()
            gdf["__index"] = gdf.index
            df1 = gdf.assign(geometry_1=gdf.geometry)
            df2 = gdf.assign(geometry_2=gdf.geometry)
            overlaps = gpd.overlay(df1, df2, how="intersection", keep_geom_type=True)
            overlaps = overlaps[overlaps["__index_1"] != overlaps["__index_2"]]
            overlaps = self._filter_significant_overlaps(
                overlaps, "geometry_1", "geometry_2", self.threshold
            )
            if not overlaps.empty:
                print(
                    f"⚠️ Significante interne overlap in {name} ({len(overlaps)} gevallen)"
                )
                if self.write_outputs["internal"]:
                    layer = f"overlap_internal_{name}"

                    def normalize_row(row):
                        pair = sorted(
                            [
                                (
                                    str(row[f"{self.gisib_id_col}_1"]),
                                    str(row[f"{self.objecttype_col}_1"]),
                                ),
                                (
                                    str(row[f"{self.gisib_id_col}_2"]),
                                    str(row[f"{self.objecttype_col}_2"]),
                                ),
                            ]
                        )
                        return pd.Series(
                            {
                                f"{self.gisib_id_col}_1": pair[0][0],
                                f"{self.objecttype_col}_1": pair[0][1],
                                f"{self.gisib_id_col}_2": pair[1][0],
                                f"{self.objecttype_col}_2": pair[1][1],
                            }
                        )

                    overlaps[
                        [
                            f"{self.gisib_id_col}_1",
                            f"{self.objecttype_col}_1",
                            f"{self.gisib_id_col}_2",
                            f"{self.objecttype_col}_2",
                        ]
                    ] = overlaps.apply(normalize_row, axis=1)
                    overlaps = (
                        overlaps.drop_duplicates(
                            subset=[
                                f"{self.gisib_id_col}_1",
                                f"{self.objecttype_col}_1",
                                f"{self.gisib_id_col}_2",
                                f"{self.objecttype_col}_2",
                            ]
                        )
                        .loc[
                            :,
                            [
                                f"{self.gisib_id_col}_1",
                                f"{self.objecttype_col}_1",
                                f"{self.gisib_id_col}_2",
                                f"{self.objecttype_col}_2",
                                "geometry",
                            ],
                        ]
                        .set_geometry("geometry")
                    )
                    # Keep only columns you want, with "geometry" as the actual intersection

                    overlaps.to_file(self.gpkg_path, layer=layer, driver="GPKG")
            results[name] = overlaps
        return results

    def validate_no_cross_overlap(self):
        """
        Checks for overlaps between each pair of asset/object types.
        Returns a dict: (type1, type2) -> overlaps_gdf
        """
        results = {}
        asset_names = list(self.assets.keys())
        for name1, name2 in combinations(asset_names, 2):
            gdf1, gdf2 = self.assets[name1].copy(), self.assets[name2].copy()
            gdf1["__index"] = gdf1.index
            gdf2["__index"] = gdf2.index
            df1 = gdf1.assign(geometry_1=gdf1.geometry)
            df2 = gdf2.assign(geometry_2=gdf2.geometry)
            overlaps = gpd.overlay(df1, df2, how="intersection", keep_geom_type=True)
            overlaps = self._filter_significant_overlaps(
                overlaps, "geometry_1", "geometry_2", self.threshold
            )
            if not overlaps.empty:
                print(
                    f"⚠️ Significante overlap tussen {name1} en {name2} ({len(overlaps)} gevallen)"
                )
                if self.write_outputs["cross"]:
                    layer = f"overlap_cross_{name1}_{name2}"

                    def normalize_row(row):
                        pair = sorted(
                            [
                                (
                                    str(row[f"{self.gisib_id_col}_1"]),
                                    str(row[f"{self.objecttype_col}_1"]),
                                ),
                                (
                                    str(row[f"{self.gisib_id_col}_2"]),
                                    str(row[f"{self.objecttype_col}_2"]),
                                ),
                            ]
                        )
                        return pd.Series(
                            {
                                f"{self.gisib_id_col}_1": pair[0][0],
                                f"{self.objecttype_col}_1": pair[0][1],
                                f"{self.gisib_id_col}_2": pair[1][0],
                                f"{self.objecttype_col}_2": pair[1][1],
                            }
                        )

                    overlaps[
                        [
                            f"{self.gisib_id_col}_1",
                            f"{self.objecttype_col}_1",
                            f"{self.gisib_id_col}_2",
                            f"{self.objecttype_col}_2",
                        ]
                    ] = overlaps.apply(normalize_row, axis=1)
                    overlaps = (
                        overlaps.drop_duplicates(
                            subset=[
                                f"{self.gisib_id_col}_1",
                                f"{self.objecttype_col}_1",
                                f"{self.gisib_id_col}_2",
                                f"{self.objecttype_col}_2",
                            ]
                        )
                        .loc[
                            :,
                            [
                                f"{self.gisib_id_col}_1",
                                f"{self.objecttype_col}_1",
                                f"{self.gisib_id_col}_2",
                                f"{self.objecttype_col}_2",
                                "geometry",
                            ],
                        ]
                        .set_geometry("geometry")
                    )
                    # Keep only columns you want, with "geometry" as the actual intersection

                    overlaps.to_file(self.gpkg_path, layer=layer, driver="GPKG")
            results[(name1, name2)] = overlaps
        return results

    def validate_overlap_by_area(self):
        """
        Checks for overlaps by area, regardless of type.
        Exports to a single layer if any found.
        """
        df = self.gisib
        for required_col in [
            f"{self.gisib_id_col}",
            self.objecttype_col,
            self.relatieve_hoogteligging_col,
        ]:
            if required_col not in df.columns:
                raise ValueError(
                    f"Kolom '{required_col}' niet gevonden in GeoDataFrame."
                )

        df1 = df.assign(geometry_1=df.geometry)
        df2 = df.assign(geometry_2=df.geometry)
        result = df1.overlay(df2, how="intersection", keep_geom_type=True)

        for col in [
            f"{self.gisib_id_col}_1",
            f"{self.gisib_id_col}_2",
            f"{self.objecttype_col}_1",
            f"{self.objecttype_col}_2",
            f"{self.relatieve_hoogteligging_col}_1",
            f"{self.relatieve_hoogteligging_col}_2",
        ]:
            if col not in result.columns:
                raise ValueError(
                    f"Kolom '{col}' niet gevonden na overlay. Controleer input-bestanden."
                )

        # Filter for actual overlaps (not self)
        result = (
            result.loc[
                result[f"{self.gisib_id_col}_1"] != result[f"{self.gisib_id_col}_2"]
            ]
            .assign(
                overlap_1=lambda d: d.geometry.area / d.geometry_1.area,
                overlap_2=lambda d: d.geometry.area / d.geometry_2.area,
            )
            .drop_duplicates(subset=["geometry"])
        )

        overlaps = (
            result.loc[
                (result.overlap_1 > self.threshold)
                | (result.overlap_2 > self.threshold)
                ]
            .assign(
                **{
                    f"{self.relatieve_hoogteligging_col}_2": lambda d: d[
                        f"{self.relatieve_hoogteligging_col}_2"
                    ].fillna(d[f"{self.relatieve_hoogteligging_col}_1"]),
                    f"{self.relatieve_hoogteligging_col}_1": lambda d: d[
                        f"{self.relatieve_hoogteligging_col}_1"
                    ].fillna(d[f"{self.relatieve_hoogteligging_col}_2"]),
                }
            )
            .fillna(
                {
                    f"{self.relatieve_hoogteligging_col}_1": 0,
                    f"{self.relatieve_hoogteligging_col}_2": 0,
                }
            )
            # 🔑 ensure integer comparison
            .assign(
                **{
                    f"{self.relatieve_hoogteligging_col}_1": lambda d: d[
                        f"{self.relatieve_hoogteligging_col}_1"
                    ].astype(int),
                    f"{self.relatieve_hoogteligging_col}_2": lambda d: d[
                        f"{self.relatieve_hoogteligging_col}_2"
                    ].astype(int),
                }
            )
            .loc[
                lambda d: d[f"{self.relatieve_hoogteligging_col}_1"]
                          == d[f"{self.relatieve_hoogteligging_col}_2"]
            ]
            .assign(
                objecttypes=lambda d: d.apply(
                    lambda row: "_".join(
                        sorted(
                            [
                                (
                                    str(row[f"{self.objecttype_col}_1"])
                                    if pd.notnull(row[f"{self.objecttype_col}_1"])
                                    else ""
                                ),
                                (
                                    str(row[f"{self.objecttype_col}_2"])
                                    if pd.notnull(row[f"{self.objecttype_col}_2"])
                                    else ""
                                ),
                            ]
                        )
                    ).lower(),
                    axis=1,
                ),
                guids=lambda d: d.apply(
                    lambda row: " ".join(
                        sorted(
                            [
                                str(row[f"{self.gisib_id_col}_1"]),
                                str(row[f"{self.gisib_id_col}_2"]),
                            ]
                        )
                    ),
                    axis=1,
                ),
            )
        )

        if not overlaps.empty:
            print(
                f"⚠️ Oppervlakte-overlap gevonden ({len(overlaps)} gevallen met >{self.threshold * 100:.0f}% overlap)"
            )
            if self.write_outputs["all"]:
                layer = "overlap_by_area"

                def normalize_row(row):
                    pair = sorted(
                        [
                            (
                                str(row[f"{self.gisib_id_col}_1"]),
                                str(row[f"{self.objecttype_col}_1"]),
                            ),
                            (
                                str(row[f"{self.gisib_id_col}_2"]),
                                str(row[f"{self.objecttype_col}_2"]),
                            ),
                        ]
                    )
                    return pd.Series(
                        {
                            f"{self.gisib_id_col}_1": pair[0][0],
                            f"{self.objecttype_col}_1": pair[0][1],
                            f"{self.gisib_id_col}_2": pair[1][0],
                            f"{self.objecttype_col}_2": pair[1][1],
                        }
                    )

                overlaps[
                    [
                        f"{self.gisib_id_col}_1",
                        f"{self.objecttype_col}_1",
                        f"{self.gisib_id_col}_2",
                        f"{self.objecttype_col}_2",
                    ]
                ] = overlaps.apply(normalize_row, axis=1)
                overlaps = (
                    overlaps.drop_duplicates(
                        subset=[
                            f"{self.gisib_id_col}_1",
                            f"{self.objecttype_col}_1",
                            f"{self.gisib_id_col}_2",
                            f"{self.objecttype_col}_2",
                        ]
                    )
                    .loc[
                        :,
                        [
                            f"{self.gisib_id_col}_1",
                            f"{self.objecttype_col}_1",
                            f"{self.gisib_id_col}_2",
                            f"{self.objecttype_col}_2",
                            "geometry",
                        ],
                    ]
                    .set_geometry("geometry")
                )
                # Keep only columns you want, with "geometry" as the actual intersection

                overlaps.to_file(self.gpkg_path, layer=layer, driver="GPKG")
                print(
                    "Overlap counts between the assets:\n\n:",
                    overlaps.loc[
                        :, [f"{self.objecttype_col}_1", f"{self.objecttype_col}_2"]
                    ].value_counts(),
                )
        return overlaps

    def run_all_validations(self):
        """
        Runs all validations, optionally writing outputs to GeoPackage.
        """
        # zonder relatieve hoogteligging
        # internals = self.validate_no_internal_overlap()
        # zonder relatieve hoogteligging
        # crosses = self.validate_no_cross_overlap()
        # met relatieve hoogteligging
        by_area = self.validate_overlap_by_area()
        return by_area
        # {
        #     "internal": internals,
        #     "cross": crosses,
        #     "by_area": by_area
        # }
