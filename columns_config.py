# schema_config.py

# Define BGT columns once
BGT_COLUMNS = [
    "type",
    "bgttype",
    "begintijd",
    "lokaalid",
    "hoogtelig",
    "optalud",
    "bronhoud",
    "bgtstatus",
    "bgtfysvkn",
    "plusfysvkn",
    "geometry",
    "fp",
]

from asset_config import AssetType

# Define per-asset GISIB columns only
ASSET_SCHEMAS = {
    AssetType.TERREINDEEL: [
        "guid",
        "objecttype",
        "object_begintijd",
        "geometry",
        "relatieve_hoogteligging",
        "type_object",
        "type_object_plus",
        "onderhoudsplichtige",
        "gbd_buurt_naam",
        "type_beheerder_plus",
    ],
    AssetType.GROENOBJECTEN: [
        "guid",
        "objecttype",
        "object_begintijd",
        "geometry",
        "relatieve_hoogteligging",
        "type_object",
        "type_object_plus",
        "onderhoudsplichtige",
        "gbd_buurt_naam",
        "type_beheerder_plus",
    ],
    AssetType.VERHARDINGEN: [
        "guid",
        "objecttype",
        "opleverdatum",
        "geometry",
        "relatieve_hoogteligging",
        "type_object",
        "type_plus",
        "onderhoudsplichtige",
        "gbd_buurt_naam",
        "type_beheerder_plus",
    ],
}
