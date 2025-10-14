# 🔑 sleutels_gisib_bgt

Een Python-tool voor het **controleren en matchen** van gemeentelijke BOR-data (Beheer Openbare Ruimte, gisib) met BGT-data (Basisregistratie Grootschalige Topografie) volgens de **Controletabel IMBOR–IMGEO**.  
Ontwikkeld voor de **Gemeente Amsterdam** in het kader van de synchronisatie en kwaliteitscontrole van objectregistraties.

---

## 🧭 Doel van dit project

De tool vergelijkt lokale BOR-objecten — zoals **groenobjecten**, **terreindelen** en **verhardingen** — met de officiële BGT-registratie.  
Daarbij controleert het programma of de combinaties tussen objecttypes geldig zijn volgens het landelijke IMBOR–IMGEO model.

De uitkomst:
- Geeft inzicht in **welke objecten correct gekoppeld** zijn tussen gisib en BGT.
- Signaleert **foutieve combinaties**.
- Produceert **GeoPackages (GPKG)** met testcases en resultaten die kunnen worden ingelezen in GIS-software (zoals QGIS of ArcGIS).

---

## 🧱 Projectstructuur

```
sleutels_gisib_bgt/
├── already_imported/              # Optionele map met reeds geïmporteerde datasets
├── matchers/                      # Match-logica voor elk objecttype (groen, terrein, verharding)
│
├── .env                           # Lokale paden naar bestanden (zie uitleg hieronder)
├── .gitignore
│
├── bucket_processor.py            # Verwerkt datasets per 'bucket'-categorie
├── buckets.py                     # Definieert buckets en schrijft resultaten weg
├── columns_config.py              # Kolomnamen en mapping-configuratie
├── controller.py                  # Centrale aansturing van het validatieproces
├── controller_utils.py            # Hulpfuncties voor controller.py
├── dataloaders.py                 # Leest datasets in (GPKG, shapefile, Excel)
├── enums.py                       # Enumeraties voor objecttypes en categorieën
├── exclude_guids.py               # Filtert objecten op basis van GUID’s die moeten worden overgeslagen
├── gebieden.py                    # Selecteert en verwerkt stadsdelen en buurten
├── gisib_validator.py             # Controleert toegestane IMBOR–IMGEO combinaties
├── global_vars.py                 # Centrale opslag van paden en constante variabelen
├── invalid_types.py               # Detecteert foutieve typecombinaties
├── main.py                        # Hoofdscript (startpunt van het programma)
├── validate_output.py             # Controleert gegenereerde outputbestanden
│
├── requirements.txt               # Vereiste Python-pakketten
└── README.md                      # Deze handleiding
```

---

## ⚙️ Installatie

### 1. Clone de repository

```bash
git clone https://github.com/ResultManagersWouter/sleutels_gisib_bgt.git
cd sleutels_gisib_bgt
```

### 2. Maak een nieuwe Python-omgeving aan

#### macOS / Linux (voorbeeld, kan anders zijn):
```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### Windows (voorbeeld, kan anders zijn):
```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Installeer de vereiste pakketten

```bash
pip install -r requirements.txt
```

---

## 🧾 Configuratie (.env)

Maak een `.env`-bestand aan in de hoofdmap van het project.  
Voeg hierin de juiste paden toe naar jouw lokale bestanden.

### Voorbeeld `.env`
```bash
FP_VRH=/Users/.../16092025_vrh.gpkg
FP_GRN=/Users/.../16092025_grn.gpkg
FP_TRD=/Users/.../16092025_trd.gpkg

FP_BGT_FOLDER=/Users/.../Zuidoost20250915
FP_GEBIEDEN=/Users/.../gebieden.gpkg
FP_CONTROLE_TABEL=/Users/.../IMBOR_IMGeo_Controletabel.xlsx

# optioneel, alleen als je al geïmporteerde bestanden wilt uitsluiten
EXCLUDE_FOLDER=/Users/.../already_imported
```

### Betekenis van de variabelen

| Variabele | Beschrijving |
|------------|--------------|
| `FP_VRH` | Verhardingsobjecten (BOR) |
| `FP_GRN` | Groenobjecten (BOR) |
| `FP_TRD` | Terreindelen (BOR) |
| `FP_BGT_FOLDER` | Map met shapefiles per BGT-beheerobject |
| `FP_GEBIEDEN` | Gebiedsdata (stadsdelen + buurten) |
| `FP_CONTROLE_TABEL` | Excel met toegestane IMBOR–IMGEO combinaties |
| `EXCLUDE_FOLDER` | (Optioneel) Map met reeds verwerkte bestanden |

---

## 🧩 Gebruikersinvoer (in `main.py`)

De gebruiker kiest zelf **welke stadsdelen** moeten worden verwerkt.  
De tool werkt zowel met één stadsdeel als met meerdere tegelijk.

```python
input_gebieden = [
    'Zuidoost',  # voorbeeld: kies één of meerdere stadsdelen, check of deze ook in de BGT en gisib zitten
    # 'Centrum', 'Noord', 'Oost', ...
]

Gebaseerd op het input gebied(en) worden de contouren ingeladen van het gebied en daarop de files ook gefiltered (ook al zijn de gebieden niet aaneensluitend).


negate = False          # False = alleen geselecteerde gebieden
                        # True = alles behalve deze gebieden

exclude_guids = False   # GUID’s overslaan die al eerder verwerkt zijn

# Instellingen voor uitvoer:
write_overlaps = True
write_manual_buckets = False
write_invalid_types = False
write_import_files = False

# Instellingen voor het aanmaken van testcases:
create_manual_buckets = False
create_invalid_types = False
```

💡 **Tip:** Voor een eerste test kun je beginnen met slechts één stadsdeel (bijv. `Zuidoost`)  
en alleen `write_overlaps = True` aanzetten. De resultaten komen in de map `output/`.

---

## 🧠 Technische uitleg per module

### 📦 `controller.py` en `controller_utils.py`
Hoofdlogica van het programma.  
Stuurt de verwerking van alle datasets aan (inlezen, matchen, filteren, schrijven).

### ⚙️ `dataloaders.py`
Laadt data uit GeoPackages, shapefiles, Excel- en CSV-bestanden.

### ✅ `gisib_validator.py`
Controleert of gisib-objecten overlap hebben. 

### 🧭 `gebieden.py`
Gebieden (stadsdelen, buurten) die mee worden kunnen meegenomen in de analyse.

### 🌿 `matchers/`
Bevat specifieke logica voor elk objecttype:
- `matcher_groenobjecten.py`
- `matcher_terreindeel.py`
- `matcher_verhardingsobjecten.py`

Alle matchers erven van `matcher_base.py` en delen dezelfde kernlogica.
Voor groenobjecten is bijvoorbeeld een uitzondering op de hagen: "filter_hagen".

### 🧹 `filter_matches.py`
Schoont ruwe matches op en verwijdert duplicaten of irrelevante records.

### 🪣 `buckets.py` & `bucket_processor.py`
Groeperen en verwerken resultaten in categorieën (buckets), bijvoorbeeld:
- `invalid_types`
- `manual_buckets`
- `overlaps`

### 🚫 `invalid_types.py`
Detecteert ongeldige combinaties volgens de controletabel.

### 🧱 `columns_config.py` & `enums.py`
Beheren de kolomnamen, veldmappings en vaste waarden (zoals type-objectcodes).

### 🔎 `validate_output.py`
Controleert of de gegenereerde outputbestanden geldig en compleet zijn.

### 🧩 `exclude_guids.py`
Verwijdert objecten die in een eerdere run al zijn verwerkt (uit `already_imported/`).

---

## 🚀 Uitvoering

1. **Controleer** dat `.env` correct is ingevuld.  
2. **Activeer** je Python-omgeving.  
3. **Start het script in interactive modus (makkelijker voor debuggen):**
   ```bash
   ipython -i main.py
   ```
4. De resultaten worden opgeslagen in de map `output/` als `.gpkg`-bestanden.  
   Deze kunnen worden geopend in QGIS of ArcGIS.

---

## 📊 Output

Afhankelijk van de ingestelde vlaggen (`write_*`) worden de volgende bestanden aangemaakt:

| Bestandstype | Beschrijving |
|---------------|--------------|
| `*_overlaps.gpkg` | Correcte matches tussen gisib en BGT |
| `*_invalid_types.gpkg` | Ongeldige combinaties |
| `*_manual_buckets.gpkg` | Handmatige controlegevallen |
---

## 🧑‍💻 Ontwikkelaar

**Result Managers met Team AGI**  
📍 Gemeente Amsterdam  
📆 Q4 2025 – Synchronisatieproject gisib ↔ BGT  

---

