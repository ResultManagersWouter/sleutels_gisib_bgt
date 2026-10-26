import geopandas as gpd
import pandas as pd
from global_vars import TRESHOLD_HAGEN, PREFERRED_BGT_OBJECTTYPE, DEBUG_PRINTS
import logging

logger = logging.getLogger(__name__)

DEBUG_COLS = ["OBJECTTYPE", "TYPE", "TYPE_GEDETAILLEERD", "ObjectType", "overlap_gisib", "overlap_bgt"]


def preferred_bgt_objecttype(intersection_df: pd.DataFrame, rules: dict = PREFERRED_BGT_OBJECTTYPE) -> pd.Series:
    """
    Geeft per rij het BGT ObjectType uit `rules` op basis van TYPE, of anders TYPE_GEDETAILLEERD (NaN als er geen regel is).
    """
    preferred = intersection_df["TYPE"].map(rules)
    if "TYPE_GEDETAILLEERD" in intersection_df.columns:
        preferred = preferred.fillna(intersection_df["TYPE_GEDETAILLEERD"].map(rules))
    return preferred


def filter_preferred_bgt_objecttype(
    intersection_df: gpd.GeoDataFrame,
    gisib_id_col: str = "GUID",
    bgt_id_col: str = "LokaalID",
    rules: dict = PREFERRED_BGT_OBJECTTYPE,
    threshold: float = TRESHOLD_HAGEN,
    label: str = "",
) -> gpd.GeoDataFrame:
    """
    Voor GISIB objecten waarvan TYPE of TYPE_GEDETAILLEERD in `rules` staat (bv. Haag, Zand):
    als het object > threshold overlapt met het BGT ObjectType uit `rules`, houd dan alleen die rij(en) over
    en gooi de overlappingen met de andere (gestapelde) BGT objecten weg.
    """
    preferred = preferred_bgt_objecttype(intersection_df, rules)

    is_preferred_match = (intersection_df["ObjectType"] == preferred) & (
        intersection_df["overlap_gisib"] > threshold
    )
    guids = intersection_df.loc[is_preferred_match, gisib_id_col].unique()
    keep = ~intersection_df[gisib_id_col].isin(guids) | is_preferred_match

    logger.info(
        "Filter op voorkeurs BGT objecttype: %d gisib objecten, %d overlappingen verwijderd",
        len(guids), (~keep).sum(),
    )
    if DEBUG_PRINTS:
        _print_debug(intersection_df, preferred, guids, keep, gisib_id_col, bgt_id_col, threshold, label)
    return intersection_df[keep].copy()


def _print_debug(df, preferred, guids, keep, gisib_id_col, bgt_id_col, threshold, label):
    cols = [gisib_id_col, bgt_id_col] + [c for c in DEBUG_COLS if c in df.columns]
    type_cols = [c for c in ["TYPE", "TYPE_GEDETAILLEERD"] if c in df.columns]
    print(f"\n========== DEBUG hagen/zand filter: {label} ==========")

    # 1. Welke TYPE waarden in deze laag lijken op haag/zand? Staat de spelling niet in PREFERRED_BGT_OBJECTTYPE,
    #    dan doet de filter niets voor die objecten.
    per_guid = df.drop_duplicates(gisib_id_col)
    lijkt_op = per_guid[type_cols].astype(str).apply(lambda s: s.str.contains("haag|zand", case=False)).any(axis=1)
    print("1. TYPE waarden die op haag/zand lijken (aantal gisib objecten):")
    if lijkt_op.any():
        print(per_guid.loc[lijkt_op].groupby(type_cols, dropna=False).size().to_string())
    else:
        print("   geen")

    # 2. Per regel: hoeveel objecten zijn gefilterd (overlap > threshold met het juiste BGT ObjectType)
    has_rule = preferred.notna()
    print(f"2. Objecten met een regel uit PREFERRED_BGT_OBJECTTYPE (gefilterd = overlap_gisib > {threshold} "
          f"met het voorkeurs ObjectType):")
    if not has_rule.any():
        print("   geen -> de filter doet niets in deze laag")
    else:
        summary = (
            df.loc[has_rule, [gisib_id_col] + type_cols]
            .assign(voorkeur_bgt=preferred[has_rule], gefilterd=lambda d: d[gisib_id_col].isin(guids))
            .drop_duplicates(gisib_id_col)
            .groupby(type_cols + ["voorkeur_bgt", "gefilterd"], dropna=False)
            .size()
        )
        print(summary.to_string())

        # 3. Objecten met een regel die NIET gefilterd zijn: geen overlap > threshold met het voorkeurs ObjectType
        niet = df.loc[has_rule & ~df[gisib_id_col].isin(guids)]
        if not niet.empty:
            voorbeeld = niet[gisib_id_col].unique()[:10]
            print(f"3. NIET gefilterd: {niet[gisib_id_col].nunique()} objecten, alle overlappingen van max 10 objecten:")
            print(niet.loc[niet[gisib_id_col].isin(voorbeeld), cols].sort_values(gisib_id_col).round(3).to_string())

    # 4. Welke overlappingen zijn weggegooid
    print(f"4. Verwijderde overlappingen: {(~keep).sum()} van {len(df)}")
    if (~keep).any():
        print(df.loc[~keep].groupby(type_cols[:1] + ["ObjectType"], dropna=False).size().to_string())

    # 5. Check vooraf: objecten die na de filter nog > 75% (beide kanten) met meerdere BGT objecten overlappen.
    #    Als ze niet eerder in een bucket vallen, geven deze de AssertionError in geom_match.
    after = df.loc[keep]
    geom = after.loc[(after["overlap_bgt"] > 0.75) & (after["overlap_gisib"] > 0.75)]
    counts = geom[gisib_id_col].value_counts()
    dup = geom.loc[geom[gisib_id_col].isin(counts[counts > 1].index)]
    if dup.empty:
        print("5. Na filter geen objecten meer met > 75% overlap met meerdere BGT objecten -> OK")
    else:
        print(f"5. Na filter nog {dup[gisib_id_col].nunique()} objecten met > 75% overlap met meerdere BGT objecten "
              f"-> kan de assert in geom_match geven (max 40 rijen):")
        print(dup[cols].sort_values(gisib_id_col).head(40).round(3).to_string())
