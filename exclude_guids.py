# place files in your folder, place folder name in .env file
# with the name EXCLUDE_FOLDER
# the file should have guid with the name GUID or guid.

# Minimal + stable GUID collector (sequential, low RAM, no threads, no pyogrio)

import os
import pandas as pd

# Requires: pip install fiona pandas openpyxl

try:
    import fiona
except ImportError as e:
    raise SystemExit("This script needs 'fiona' installed. Try: pip install fiona") from e

GUID_NAMES = {"guid"}  # extend if you use variants (e.g., 'globalid')
def pick_guid_col(cols):
    cols = [str(c) for c in cols]
    # exact first
    for c in cols:
        if c.strip().lower() in GUID_NAMES:
            return c
    # then contains 'guid'
    for c in cols:
        if "guid" in c.lower():
            return c
    return None

def normalize_guid(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def guids_from_gpkg(path):
    """Yield GUIDs from every layer of a GeoPackage using Fiona, streaming features."""
    try:
        layers = fiona.listlayers(path)
    except Exception as e:
        print(f"[WARN] Cannot list layers in {path}: {e}")
        return

    for layer in layers:
        try:
            with fiona.open(path, layer=layer) as src:
                # find GUID-like property name
                props = list(src.schema.get("properties", {}).keys())
                gcol = pick_guid_col(props)
                if not gcol:
                    continue
                for feat in src:
                    g = normalize_guid(feat["properties"].get(gcol))
                    if g:
                        yield g
        except Exception as e:
            print(f"[WARN] Could not read {path}::{layer}: {e}")

def guids_from_excel(path, exclude_sheets=("remove",)):
    """Yield GUIDs from all sheets (except excluded) using pandas (openpyxl engine for xlsx)."""
    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        print(f"[WARN] Cannot open {path}: {e}")
        return

    exclude = {s.lower() for s in exclude_sheets}
    for sheet in xl.sheet_names:
        if sheet.lower() in exclude:
            continue
        try:
            # header only to find GUID column
            header_df = xl.parse(sheet_name=sheet, nrows=0)
        except Exception as e:
            print(f"[WARN] Could not read header of {path}::{sheet}: {e}")
            continue

        gcol = pick_guid_col(header_df.columns)
        if not gcol:
            continue

        try:
            # read only that column
            df = xl.parse(
                sheet_name=sheet,
                usecols=lambda c: str(c).strip().lower() == str(gcol).strip().lower(),
                dtype="string",
            )
            col = df.columns[0]
            for v in df[col].dropna().tolist():
                g = normalize_guid(v)
                if g:
                    yield g
        except Exception as e:
            print(f"[WARN] Could not read {path}::{sheet}: {e}")

def collect_all_guids(folder, exclude_sheets=("remove",)):
    """Return a deduplicated list of GUIDs from all GPKGs and Excels under folder."""
    unique = set()
    for root, _, files in os.walk(folder):
        for fname in files:
            path = os.path.join(root, fname)
            low = fname.lower()
            if low.endswith(".gpkg"):
                for g in guids_from_gpkg(path):
                    unique.add(g)
            elif low.endswith((".xlsx", ".xlsm", ".xls")):
                for g in guids_from_excel(path, exclude_sheets):
                    unique.add(g)
    return list(unique)

# Example:
# folder = "/path/to/folder"
# guids = collect_all_guids(folder)
# print(len(guids), "unique GUIDs")
# guids[:10]
