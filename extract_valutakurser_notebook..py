# Databricks notebook source
# =================================================================
# JOBB-STEG 1/2: Valutakurser — EXTRACT til landing-sonen
# =================================================================
# Dette er original-jobbens STEG 1, trimmet til å KUN gjøre ekstraksjon.
# Kjøres som første Task i en Databricks Job, FØR DLT-pipelinen
# (dlt_pipeline_valutakurs.py) som er neste Task i samme jobb.
#
# STEG 2-4 (Autoloader-lesing, flatening, MERGE, kolonnekommentarer
# og sensitivitets-tag) eies nå av DLT-pipelinen selv - se DEL 5/6-
# kommentaren nederst for hvordan de to gjenværende bitene (metadata-
# kommentarer og tag) håndteres når skrivingen skjer via DLT.
#
# MÅLTABELL (kun for AUTO-deteksjon av lastetype, se DEL 4):
# {KATALOG}.dlt_bronse.valutakurs
# =================================================================

import requests
import json
from datetime import date, datetime, timedelta


# -----------------------------------------------------------------
# DEL 1: PARAMETER — lastetype
# -----------------------------------------------------------------
dbutils.widgets.dropdown(
    "lastetype", "full", ["auto", "inkrementell", "full"], "Lastetype"
)
LASTETYPE_PARAMETER = dbutils.widgets.get("lastetype").strip().lower()

if LASTETYPE_PARAMETER not in ("auto", "inkrementell", "full"):
    raise ValueError(
        f"Ugyldig lastetype: '{LASTETYPE_PARAMETER}'. "
        "Må være 'auto', 'inkrementell' eller 'full'."
    )

print(f"ℹ️ Parameter 'lastetype' satt til: '{LASTETYPE_PARAMETER}'")


# -----------------------------------------------------------------
# DEL 1b: ØVRIGE PARAMETERE
# -----------------------------------------------------------------
dbutils.widgets.text("valutaer", "EUR,SEK,USD", "Valutaer (kommaseparert)")
VALUTAER_PARAMETER = dbutils.widgets.get("valutaer").strip()
VALUTAER = [v.strip().upper() for v in VALUTAER_PARAMETER.split(",") if v.strip()]

if not VALUTAER:
    raise ValueError(
        f"Ugyldig parameter 'valutaer': '{VALUTAER_PARAMETER}'. "
        "Må være en kommaseparert liste, f.eks. 'EUR,SEK,USD'."
    )

dbutils.widgets.text("full_start_periode", "2026-01-01", "Full last - startdato")
FULL_START_PERIODE = dbutils.widgets.get("full_start_periode").strip()

try:
    datetime.strptime(FULL_START_PERIODE, "%Y-%m-%d")
except ValueError:
    raise ValueError(
        f"Ugyldig parameter 'full_start_periode': '{FULL_START_PERIODE}'. "
        "Må være på format 'YYYY-MM-DD', f.eks. '2026-01-01'."
    )

dbutils.widgets.text("katalog", "test_dab", "Katalog (Unity Catalog)")
KATALOG = dbutils.widgets.get("katalog").strip()

print(f"ℹ️ Parameter 'valutaer' satt til:           {VALUTAER}")
print(f"ℹ️ Parameter 'full_start_periode' satt til: '{FULL_START_PERIODE}'")
print(f"ℹ️ Parameter 'katalog' satt til:            '{KATALOG}'")


# -----------------------------------------------------------------
# DEL 2: KONFIGURASJON
# -----------------------------------------------------------------
# NB: ingen CHECKPOINT_PATH her lenger - DLT-pipelinen eier sitt eget
# checkpoint internt. Denne notebooken bryr seg kun om landing-sonen.
# -----------------------------------------------------------------
LANDING_SKJEMA = "landing"
LANDING_VOLUM  = "valuta"
BRONSE_SKJEMA  = "dlt_bronse"   # <-- endret fra 'bronse'
BRONSE_TABELL  = "valutakurs"

LANDING_PATH = f"/Volumes/{KATALOG}/{LANDING_SKJEMA}/{LANDING_VOLUM}"
TABELLNAVN   = f"{KATALOG}.{BRONSE_SKJEMA}.{BRONSE_TABELL}"

print("=" * 65)
print("KONFIGURASJON")
print("=" * 65)
print(f"LANDING_PATH: {LANDING_PATH}")
print(f"TABELLNAVN:   {TABELLNAVN}  (eies/skrives av DLT-pipelinen)")


# -----------------------------------------------------------------
# DEL 3: SIKRE AT SKJEMA OG VOLUM FINNES
# -----------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {KATALOG}.{LANDING_SKJEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {KATALOG}.{LANDING_SKJEMA}.{LANDING_VOLUM}")
dbutils.fs.mkdirs(LANDING_PATH)

print("\n✅ Skjema og volum verifisert/opprettet.")
# NB: dlt_bronse-schemaet opprettes av DLT-pipelinen selv ved første
# kjøring (styrt av pipeline-innstillingene "Catalog"/"Target schema"),
# så det opprettes bevisst IKKE her.


# -----------------------------------------------------------------
# DEL 4: BESTEM ENDELIG LASTETYPE
# -----------------------------------------------------------------
if LASTETYPE_PARAMETER in ("inkrementell", "full"):
    LASTETYPE = LASTETYPE_PARAMETER
    print(f"\nℹ️ Lastetype eksplisitt satt via parameter -> '{LASTETYPE.upper()}'.")
else:  # "auto"
    tabell_finnes = spark.catalog.tableExists(TABELLNAVN)

    if not tabell_finnes:
        LASTETYPE = "full"
        print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} finnes ikke ennå -> FULL LAST.")
    else:
        antall_rader = spark.table(TABELLNAVN).limit(1).count()
        if antall_rader == 0:
            LASTETYPE = "full"
            print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} er tom -> FULL LAST.")
        else:
            LASTETYPE = "inkrementell"
            print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} har data -> INKREMENTELL LAST.")


# ===================================================================
# STEG 1 — EXTRACT: Hent valutakurser fra Norges Bank til landing-sonen
# ===================================================================
print("\n" + "=" * 65)
print("STEG 1: EXTRACT -> Landing-sonen")
print("=" * 65)

if LASTETYPE == "full":
    START_PERIODE = FULL_START_PERIODE
    print(f"ℹ️ Full last: henter fra fast startdato {START_PERIODE}.")
else:
    eksisterende_filer = [f.name for f in dbutils.fs.ls(LANDING_PATH)] \
        if any(True for _ in dbutils.fs.ls(LANDING_PATH)) else []

    siste_dato = None
    for filnavn in eksisterende_filer:
        try:
            deler = filnavn.split("_")
            dato_del = deler[2]
            fil_dato = datetime.strptime(dato_del, "%Y-%m-%d").date()
            if siste_dato is None or fil_dato > siste_dato:
                siste_dato = fil_dato
        except (IndexError, ValueError):
            continue

    if siste_dato is None:
        START_PERIODE = FULL_START_PERIODE
        print(f"ℹ️ Ingen tidligere filer funnet. Bruker startdato {START_PERIODE}.")
    else:
        START_PERIODE = str(siste_dato + timedelta(days=1))
        print(f"ℹ️ Siste hentede dato funnet: {siste_dato}. Henter fra {START_PERIODE}.")

SLUTT_PERIODE = str(date.today())
HOPP_OVER_EXTRACT = START_PERIODE > SLUTT_PERIODE
if HOPP_OVER_EXTRACT:
    print(f"✅ Ingen nye perioder å hente (start {START_PERIODE} > slutt {SLUTT_PERIODE}).")

if not HOPP_OVER_EXTRACT:
    valuta_str = "+".join(VALUTAER)
    url = f"https://data.norges-bank.no/api/data/EXR/B.{valuta_str}.NOK.SP"
    params = {
        "startPeriod": START_PERIODE, "endPeriod": SLUTT_PERIODE,
        "format": "sdmx-json", "locale": "no",
    }
    response = requests.get(url, params=params)
    print("Hentet URL:", response.url)

    if response.status_code == 404:
        print("✅ Ingen data tilgjengelig for perioden (404 — sannsynligvis helg/helligdag).")
        HOPP_OVER_EXTRACT = True
    else:
        response.raise_for_status()
        data = response.json()
        antall_serier = len(data.get("data", {}).get("dataSets", [{}])[0].get("series", {}))
        print(f"Antall serier i respons: {antall_serier}")

        if antall_serier == 0:
            if LASTETYPE == "full":
                raise ValueError(
                    f"Ingen data returnert for full last, periode {START_PERIODE} "
                    f"til {SLUTT_PERIODE}. Sjekk VALUTAER og START_PERIODE."
                )
            else:
                print("✅ Ingen nye observasjoner i perioden.")
                HOPP_OVER_EXTRACT = True

if not HOPP_OVER_EXTRACT:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filnavn = f"valutakurser_{LASTETYPE}_{SLUTT_PERIODE}_{timestamp}.json"
    filsti  = f"{LANDING_PATH}/{filnavn}"
    dbutils.fs.put(filsti, json.dumps(data, ensure_ascii=False), overwrite=False)
    print(f"✅ Lagret rådata ({LASTETYPE}) til: {filsti}")
else:
    print("ℹ️ Extract-steget produserte ingen ny fil i landing-sonen denne kjøringen.")

print("\n" + "=" * 65)
print(f"EXTRACT FULLFØRT — lastetype denne kjøringen: {LASTETYPE.upper()}")
print("Neste Task i jobben: DLT-pipelinen leser landing-sonen og")
print(f"skriver til {TABELLNAVN}.")
print("=" * 65)

# -----------------------------------------------------------------
# Om DEL 5/6 fra original-jobben (kolonnekommentarer og
# sensitivitets-tag på bronse-tabellen):
# - Tabellkommentar og tabell-tag ('sensitivitet'='Offentlig') er
#   satt direkte i DLT-pipelinen (comment= / table_properties= på
#   dlt.create_streaming_table), så de trenger ikke gjentas her.
# - Per-kolonne COMMENT (ALTER TABLE ... ALTER COLUMN ... COMMENT)
#   støttes ikke direkte i DLT sin Python-API. Ønsker dere det, kan
#   dere legge til et lite ETTERFØLGENDE Task nr. 3 i jobben (etter
#   DLT-pipelinen) som bare kjører DEL 5-blokken fra original-koden
#   mot {KATALOG}.dlt_bronse.valutakurs.
# -----------------------------------------------------------------
