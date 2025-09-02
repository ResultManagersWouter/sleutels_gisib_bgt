from enum import Enum
import pandas as pd
class AssetType(Enum):
    TERREINDEEL = "terreindeel"
    GROENOBJECTEN = "groenobjecten"
    VERHARDINGEN = "verhardingen"
df = pd.read_csv("gebieden_naam.csv")

class ControleTabelGisib(Enum):
    VERHARDINGSOBJECT = 'Verhardingsobject'
    TERREINDEEL = 'Terreindeel'
    GROENOBJECT = 'Groenobject'

class ObjectType(Enum):
    CONTROLE_TABEL_GISIB_OBJECT = 'Objecttype'
    BGTOBJECTTYPE = 'ObjectType'




