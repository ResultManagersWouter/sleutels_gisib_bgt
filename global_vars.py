from datetime import date
# Pas aan
SKIP_TYPES = {"Rietland", "Moeras"}
TYPE_COL_GISIB= "TYPE"
gisib_id_col = "GUID"
bgt_id_col = "LokaalID"
gisib_hoogteligging_col = "RELATIEVE_HOOGTELIGGING"
bgt_hoogteligging_col = "Nivo"
gisib_objecttype_col = "OBJECTTYPE"
TRESHOLD_HAGEN = 0.75
# GISIB TYPE of TYPE_GEDETAILLEERD -> BGT ObjectType waar het object altijd mee moet matchen.
# Nodig waar BGT objecten op elkaar liggen (bv. een Vegetatieobject haag op een Onbegroeid terreindeel),
# anders matcht het GISIB object met beide en faalt de assert in geom_match.
PREFERRED_BGT_OBJECTTYPE = {
    "Haag": "Vegetatieobject",
    "Zandvlakte": "Onbegroeid terreindeel",
}
# Print debug info van de hagen/zand filter en de geom_match assert. Zet op False als alles goed gaat.
DEBUG_PRINTS = True
THRESHOLD_OVERLAP = 0.5
today = date.today().isoformat()