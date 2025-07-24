from enum import Enum

class AssetType(Enum):
    TERREINDEEL = "terreindeel"
    GROENOBJECTEN = "groenobjecten"
    VERHARDINGEN = "verhardingen"

class Gebied(Enum):
    BUURT = "gbd_buurt_naam"
    WIJK = "gbd_wijk_naam"
    STADSDEEL = "gbd_stadsdeel_naam"