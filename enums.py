from enum import Enum

class AssetType(Enum):
    TERREINDEEL = "terreindeel"
    GROENOBJECTEN = "groenobjecten"
    VERHARDINGEN = "verhardingen"

class Gebied(Enum):
    BUURT = "BUURT"
    WIJK = "WIJK"
    STADSDEEL = "STADSDEEL_OF_KERN"

class ControleTabelGisib(Enum):
    VERHARDINGSOBJECT = 'Verhardingsobject'
    TERREINDEEL = 'Terreindeel'
    GROENOBJECT = 'Groenobject'

class ObjectType(Enum):
    CONTROLE_TABEL_GISIB_OBJECT = 'Objecttype'
    CONTROLE_TABEL_BGT_OBJECT = 'BGT Objecttype'
    BGTOBJECTTYPE = 'ObjectType'




