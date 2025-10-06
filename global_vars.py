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
THRESHOLD_OVERLAP = 0.5
today = date.today().isoformat()