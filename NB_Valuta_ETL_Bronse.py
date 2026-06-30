# =================================================================
# JOBB: Valutakurser — Landing → Bronse (valutakurs, med flatening + MERGE-dedup)
# =================================================================
# Denne notebooken kjører hele pipelinen i sekvens, ÉN samlet jobb:
#
#   STEG 1 (EXTRACT):    Henter valutakurser fra Norges Banks API og
#                         lagrer rå JSON i landing-sonen.
#   STEG 2 (AUTOLOADER):  Leser nye JSON-filer fra landing-sonen
#                         (rå struktur, ufiltrert).
#   STEG 3 (FLATENING):   Flater ut SDMX-strukturen til én rad per
#                         valuta+dato-observasjon.
#   STEG 4 (MERGE):       Skriver til bronse-tabellen 'valutakurs' med
#                         MERGE INTO, slik at kun NYE valuta+dato-
#                         kombinasjoner legges til. Eksisterende rader
#                         berøres ikke.
#
# MÅLTABELL: test_dab.bronse.valutakurs (NB: eget checkpoint - bygges fra scratch)
#
# LASTETYPE styres av widget-parameteren "lastetype" (se DEL 1), som
# kan settes manuelt eller via en jobb/schedule i Databricks:
#   - "full"           -> (standard her) tvinger full last uansett -
#                         naturlig startpunkt siden dette er en
#                         helt ny tabell/checkpoint.
#   - "inkrementell"   -> tvinger inkrementell last uansett.
#   - "auto"            -> full ved tom/manglende tabell, ellers
#                         inkrementell. Se DEL 4.
#
# ØVRIGE PARAMETERE (se DEL 1b), alle settbare som Job Parameters:
#   - "valutaer"            -> kommaseparert liste, f.eks. "USD, EUR,SEK"
#   - "full_start_periode"  -> startdato for full last, "YYYY-MM-DD"
#   - "katalog"             -> hvilken Unity Catalog-katalog jobben
#                              kjører mot, f.eks. "test_dab"
#
# LASTETDATO-KOLONNEN:
# 'LastetDato' viser nøyaktig tidspunktet (dato + klokkeslett) da en
# gitt rad - dvs. en unik kombinasjon av dato, valuta og kurs-verdi -
# faktisk ble lastet inn i bronse-tabellen. Den settes KUN ved selve
# innsettingen (i MERGE-steget sin whenNotMatchedInsertAll-gren) og
# endres ALDRI senere for samme rad, siden eksisterende rader aldri
# oppdateres eller overskrives i denne pipelinen.
#
# VIKTIG:
# Bronse-tabellen lagrer UTFLATEDE data (én rad per valuta+dato), IKKE
# rå JSON-blobs. Dette er nødvendig for å kunne sjekke om en
# observasjon (valuta+dato) finnes fra før, slik MERGE-logikken
# krever.
# =================================================================


# -----------------------------------------------------------------
# DEL 0: IMPORTER
# -----------------------------------------------------------------
import requests
import json
from datetime import date, datetime, timedelta

from pyspark.sql.functions import (
    current_timestamp, col, regexp_extract, to_timestamp, lit
)
from delta.tables import DeltaTable


# -----------------------------------------------------------------
# DEL 1: PARAMETER — lastetype (styres via widget, kan settes fra jobb/schedule)
# -----------------------------------------------------------------
# Verdier:
#   "auto"          -> (standard) notebooken avgjør selv: full hvis
#                       bronse-tabellen ikke finnes/er tom, ellers
#                       inkrementell. Se DEL 3.
#   "inkrementell"   -> tvinger inkrementell last, uavhengig av
#                       tabellens tilstand.
#   "full"           -> tvinger full last, uavhengig av tabellens
#                       tilstand.
#
# Når notebooken kjøres som et Task i en Databricks Workflow, kan
# denne parameteren settes under Task-konfigurasjonen (Parameters),
# eller overstyres pr. schedule/manuell kjøring uten å endre koden.
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
# DEL 1b: ØVRIGE PARAMETERE (styres via widget, kan settes fra jobb/schedule)
# -----------------------------------------------------------------
# Disse gjør det mulig å justere hvilke valutaer som hentes, hvilket
# katalog/miljø jobben kjører mot, og fast startdato for full last -
# alt uten å måtte endre selve notebook-koden. Settes som Job
# Parameters i Databricks Workflows (se forklaring i kommentar under
# hver widget).
# -----------------------------------------------------------------

# --- Valutaer: kommaseparert streng, f.eks. "EUR,SEK" ---
# Job Parameters sendes alltid inn som tekststrenger, så en liste må
# representeres som en kommaseparert streng og splittes opp her.
dbutils.widgets.text("valutaer", "EUR,SEK,USD", "Valutaer (kommaseparert)")
VALUTAER_PARAMETER = dbutils.widgets.get("valutaer").strip()
VALUTAER = [v.strip().upper() for v in VALUTAER_PARAMETER.split(",") if v.strip()]

if not VALUTAER:
    raise ValueError(
        f"Ugyldig parameter 'valutaer': '{VALUTAER_PARAMETER}'. "
        "Må være en kommaseparert liste, f.eks. 'EUR,SEK,USD'."
    )

# --- Fast startdato for full last ---
dbutils.widgets.text("full_start_periode", "2026-01-01", "Full last - startdato")
FULL_START_PERIODE = dbutils.widgets.get("full_start_periode").strip()

try:
    datetime.strptime(FULL_START_PERIODE, "%Y-%m-%d")
except ValueError:
    raise ValueError(
        f"Ugyldig parameter 'full_start_periode': '{FULL_START_PERIODE}'. "
        "Må være på format 'YYYY-MM-DD', f.eks. '2026-01-01'."
    )

# --- Katalog (Unity Catalog) jobben kjører mot ---
# Nyttig for å kunne peke samme notebook mot et annet miljø
# (f.eks. en katalog for testing) uten kodeendring.
dbutils.widgets.text("katalog", "test_dab", "Katalog (Unity Catalog)")
KATALOG = dbutils.widgets.get("katalog").strip()

print(f"ℹ️ Parameter 'valutaer' satt til:           {VALUTAER}")
print(f"ℹ️ Parameter 'full_start_periode' satt til: '{FULL_START_PERIODE}'")
print(f"ℹ️ Parameter 'katalog' satt til:            '{KATALOG}'")


# -----------------------------------------------------------------
# DEL 2: KONFIGURASJON
# -----------------------------------------------------------------
# NB: KATALOG, VALUTAER og FULL_START_PERIODE er allerede satt i
# DEL 1b ovenfor (fra widget-parametere). Resten av konfigurasjonen
# under er stabile navn som sjeldent endres, og holdes derfor som
# vanlige konstanter (ikke parametere) for å unngå unødvendig mange
# innstillinger å holde styr på i jobb-konfigurasjonen.
# -----------------------------------------------------------------
LANDING_SKJEMA     = "landing"
LANDING_VOLUM      = "valuta"
CHECKPOINT_VOLUM   = "_checkpoints"
BRONSE_SKJEMA      = "bronse"
BRONSE_TABELL      = "valutakurs"

LANDING_PATH       = f"/Volumes/{KATALOG}/{LANDING_SKJEMA}/{LANDING_VOLUM}"
# Eget, nytt checkpoint for 'valutakurs' - MÅ være forskjellig fra et
# eventuelt eldre checkpoint brukt mot 'valutakurser', siden Autoloader
# sin fil-sporing (hvilke filer som er lest) er knyttet til nettopp
# denne stien. Gjenbruk av et gammelt checkpoint mot en ny tabell ville
# fått Autoloader til å tro den allerede har lest filer den i
# realiteten ikke har skrevet til DENNE tabellen.
CHECKPOINT_PATH    = f"/Volumes/{KATALOG}/{LANDING_SKJEMA}/{CHECKPOINT_VOLUM}/valuta_bronse_valutakurs"
TABELLNAVN         = f"{KATALOG}.{BRONSE_SKJEMA}.{BRONSE_TABELL}"

print("=" * 65)
print("KONFIGURASJON")
print("=" * 65)
print(f"LANDING_PATH:    {LANDING_PATH}")
print(f"CHECKPOINT_PATH: {CHECKPOINT_PATH}")
print(f"TABELLNAVN:      {TABELLNAVN}")


# -----------------------------------------------------------------
# DEL 3: SIKRE AT SKJEMA OG VOLUM FINNES (idempotent oppsett)
# -----------------------------------------------------------------
# "Idempotent" betyr at det er trygt å kjøre disse kommandoene flere
# ganger - resultatet blir det samme uansett om skjema/volum allerede
# finnes eller ikke. IF NOT EXISTS gjør at kommandoen bare hopper over
# seg selv (i stedet for å feile) dersom ressursen allerede er
# opprettet fra en tidligere kjøring.
# -----------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {KATALOG}.{LANDING_SKJEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {KATALOG}.{BRONSE_SKJEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {KATALOG}.{LANDING_SKJEMA}.{LANDING_VOLUM}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {KATALOG}.{LANDING_SKJEMA}.{CHECKPOINT_VOLUM}")
dbutils.fs.mkdirs(LANDING_PATH)

print("\n✅ Skjema og volum verifisert/opprettet.")


# -----------------------------------------------------------------
# DEL 4: BESTEM ENDELIG LASTETYPE
# -----------------------------------------------------------------
# Hvis parameteren er satt eksplisitt til "inkrementell" eller "full"
# (f.eks. fra en jobb/schedule), brukes den verdien direkte - ingen
# auto-deteksjon utføres, og tabellens tilstand er irrelevant for
# valget.
#
# Hvis parameteren er "auto" (standard ved manuell kjøring), faller
# vi tilbake til samme smarte deteksjon som tidligere: full last hvis
# bronse-tabellen ikke finnes eller er tom, ellers inkrementell.
# -----------------------------------------------------------------
if LASTETYPE_PARAMETER in ("inkrementell", "full"):
    LASTETYPE = LASTETYPE_PARAMETER
    print(f"\nℹ️ Lastetype eksplisitt satt via parameter -> '{LASTETYPE.upper()}' "
          "(auto-deteksjon hoppet over).")

else:  # LASTETYPE_PARAMETER == "auto"
    tabell_finnes = spark.catalog.tableExists(TABELLNAVN)

    if not tabell_finnes:
        LASTETYPE = "full"
        print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} finnes ikke ennå -> kjører FULL LAST.")
    else:
        antall_rader = spark.table(TABELLNAVN).limit(1).count()
        if antall_rader == 0:
            LASTETYPE = "full"
            print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} finnes, men er tom -> kjører FULL LAST.")
        else:
            LASTETYPE = "inkrementell"
            print(f"\nℹ️ [auto] Tabellen {TABELLNAVN} har data fra før -> kjører INKREMENTELL LAST.")


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
        print(f"ℹ️ Ingen tidligere inkrementelle filer funnet. Bruker startdato {START_PERIODE}.")
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
    filnavn   = f"valutakurser_{LASTETYPE}_{SLUTT_PERIODE}_{timestamp}.json"
    filsti    = f"{LANDING_PATH}/{filnavn}"
    dbutils.fs.put(filsti, json.dumps(data, ensure_ascii=False), overwrite=False)
    print(f"✅ Lagret rådata ({LASTETYPE}) til: {filsti}")
else:
    print("ℹ️ Extract-steget produserte ingen ny fil i landing-sonen denne kjøringen.")


# ===================================================================
# STEG 2 — AUTOLOADER: Les nye JSON-filer (rå struktur) fra landing
# ===================================================================
print("\n" + "=" * 65)
print("STEG 2: AUTOLOADER -> Leser rå JSON fra landing-sonen")
print("=" * 65)

raa_df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(LANDING_PATH)
      .withColumn("_kildefil", col("_metadata.file_path"))
      .withColumn("_kildefil_navn", col("_metadata.file_name"))
)


# ===================================================================
# STEG 3 + 4 — FLATENING + MERGE: gjøres inni foreachBatch
# ===================================================================
# Vi bruker foreachBatch fordi MERGE INTO ikke kan kjøres direkte på
# en streaming-dataframe — MERGE er en batch-operasjon. foreachBatch
# gir oss én vanlig (statisk) DataFrame per micro-batch, som vi kan
# flate ut og MERGE-e som vanlig batch-kode.
# -------------------------------------------------------------------

def flat_og_merge(batch_df, batch_id):
    """
    Kjøres for hver micro-batch Autoloader produserer.

    1. Flater ut SDMX-strukturen (data.dataSets[].series.observations)
       til én rad per valuta + dato + kurs.
    2. Kobler observasjons-indeksene til faktisk valutakode via
       data.structure.dimensions (serie-dimensjonen).
    3. MERGE INTO bronse-tabellen på (valuta, dato) - kun rader som
       IKKE finnes fra før (ny valuta+dato-kombinasjon) settes inn.
       Eksisterende rader oppdateres IKKE (ren append av nye, ingen
       overskriving), i tråd med Alternativ A for LastetDato.
    """
    if batch_df.limit(1).count() == 0:
        print(f"[Batch {batch_id}] Tom batch, ingen handling.")
        return

    # --- 3.1 Hent ut rå rader til driver for parsing ---
    # SDMX-strukturen er kompleks og varierer i antall serier/valutaer,
    # så vi parser den med vanlig Python (pr. fil/respons) heller enn
    # rene Spark-funksjoner, siden mappingen mellom serie-indeks og
    # valutakode ligger i en egen del av JSON-strukturen.
    rader = batch_df.select(
        "data", "meta", "_kildefil", "_kildefil_navn"
    ).collect()

    flatede_rader = []

    for rad in rader:
        respons = rad["data"].asDict(recursive=True)
        kildefil = rad["_kildefil"]
        kildefil_navn = rad["_kildefil_navn"]

        try:
            dataset = respons["dataSets"][0]
            struktur = respons["structure"]

            # Finn hvilken dimensjon i "series"-nøkkelen som er valutaen,
            # og bygg en indeks -> valutakode-mapping.
            serie_dimensjoner = struktur["dimensions"]["series"]
            valuta_dim_indeks = next(
                i for i, d in enumerate(serie_dimensjoner) if d["id"] == "BASE_CUR"
            )
            valuta_verdier = serie_dimensjoner[valuta_dim_indeks]["values"]

            # Observasjons-dimensjonen (tid) gir oss indeks -> faktisk dato.
            obs_dimensjoner = struktur["dimensions"]["observation"]
            tid_verdier = obs_dimensjoner[0]["values"]

            for serie_nokkel, serie_innhold in dataset["series"].items():
                # serie_nokkel har format som "0:0:0:0" - ett tall per dimensjon
                indekser = [int(x) for x in serie_nokkel.split(":")]
                valuta_kode = valuta_verdier[indekser[valuta_dim_indeks]]["id"]

                for tid_indeks_str, observasjon in serie_innhold["observations"].items():
                    tid_indeks = int(tid_indeks_str)
                    dato_str = tid_verdier[tid_indeks]["id"]
                    kurs_verdi = float(observasjon[0])

                    flatede_rader.append({
                        "valuta": valuta_kode,
                        "dato": dato_str,
                        "kurs": kurs_verdi,
                        "_kildefil": kildefil,
                        "_kildefil_navn": kildefil_navn,
                    })

        except (KeyError, IndexError, StopIteration) as e:
            print(f"⚠️ Kunne ikke parse respons fra {kildefil_navn}: {e}")
            continue

    if not flatede_rader:
        print(f"[Batch {batch_id}] Ingen rader etter flatening, ingen MERGE utført.")
        return

    # --- 3.2 Bygg flatet DataFrame med riktig typer og sporingskolonner ---
    flatet_df = (
        spark.createDataFrame(flatede_rader)
        .withColumn("dato", col("dato").cast("date"))
        .withColumn(
            "HentetTidspunkt",
            to_timestamp(
                regexp_extract(col("_kildefil_navn"), r"_(\d{8}_\d{6})\.json$", 1),
                "yyyyMMdd_HHmmss"
            )
        )
        # LastetDato = datoen/tidspunktet raden - altså den unike
        # kombinasjonen av valuta, dato og kurs - FAKTISK blir lastet
        # inn i bronse-tabellen 'valutakurs'. Satt kun her, i samme
        # øyeblikk som raden bygges, FØR den når MERGE-steget.
        # Siden MERGE-steget kun har en whenNotMatchedInsertAll-gren
        # (se 3.3), blir denne verdien aldri overskrevet i ettertid
        # for en rad som allerede finnes i tabellen.
        .withColumn("LastetDato", current_timestamp())
        .dropDuplicates(["valuta", "dato"])  # dedup INNAD i samme batch
        # Eksplisitt kolonnerekkefølge: de forretningsmessig relevante
        # feltene (valuta, dato, kurs, sporingstidspunkt) først, og de
        # tekniske kildefil-referansene (_kildefil, _kildefil_navn)
        # plassert sist - siden de er metadata om HVOR raden kom fra,
        # ikke selve observasjonen.
        .select(
            "valuta",
            "dato",
            "kurs",
            "HentetTidspunkt",
            "LastetDato",
            "_kildefil",
            "_kildefil_navn",
        )
    )

    # --- 3.3 MERGE INTO bronse-tabellen på (valuta, dato) ---
    # whenNotMatchedInsertAll -> kun rader som IKKE finnes fra før
    # (basert på valuta+dato) settes inn. Vi har bevisst INGEN
    # whenMatchedUpdate-gren, siden eksisterende rader aldri skal
    # endres eller overskrives - kun nye observasjoner skal legges til.
    if spark.catalog.tableExists(TABELLNAVN):
        bronse_tabell = DeltaTable.forName(spark, TABELLNAVN)
        (bronse_tabell.alias("mål")
            .merge(
                flatet_df.alias("kilde"),
                "`mål`.valuta = kilde.valuta AND `mål`.dato = kilde.dato"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[Batch {batch_id}] MERGE utført mot eksisterende tabell. "
              f"{flatet_df.count()} kandidatrader vurdert.")
    else:
        # Tabellen finnes ikke ennå -> første skriving, vanlig opprettelse.
        flatet_df.write.format("delta").saveAsTable(TABELLNAVN)
        print(f"[Batch {batch_id}] Tabell {TABELLNAVN} opprettet med "
              f"{flatet_df.count()} rader (første kjøring).")


# -----------------------------------------------------------------
# Kjør streamen med foreachBatch
# -----------------------------------------------------------------
(raa_df.writeStream
    .foreachBatch(flat_og_merge)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

print(f"\n✅ Flatening og MERGE fullført mot {TABELLNAVN}.")


# -----------------------------------------------------------------
# DEL 5: SETT KOLONNEKOMMENTARER (metadata) på bronse-tabellen
# -----------------------------------------------------------------
# Dokumenterer hva hvert felt betyr direkte i Unity Catalog, synlig
# via DESCRIBE TABLE EXTENDED eller Catalog Explorer. Kjøres etter at
# tabellen er skrevet til (DEL 3+4), slik at ALTER COLUMN ikke feiler
# på en tabell som ennå ikke finnes. Trygt å kjøre på nytt hver gang
# jobben kjører - COMMENT overskrives bare med samme verdi.
# -----------------------------------------------------------------
print("\n" + "=" * 65)
print("DEL 5: Setter kolonnekommentarer (metadata)")
print("=" * 65)

if spark.catalog.tableExists(TABELLNAVN):
    kolonne_kommentarer = {
        "valuta": (
            "Valutakode, f.eks. USD, EUR eller SEK."        ),
        "dato": (
            "Datoen kursen gjelder for."
        ),
        "kurs": (
            "Hvor mange norske kroner (NOK) valutaen er verdt på denne datoen."
        ),
        "HentetTidspunkt": (
            "Når dataene ble hentet fra Norges Bank."
        ),
        "LastetDato": (
            "Når raden ble lagt inn i tabellen. Endres aldri senere."
        ),
        "_kildefil": (
            "Hvilken fil i landing-sonen denne raden kommer fra (full filsti)."
        ),
        "_kildefil_navn": (
            "Samme som _kildefil, men kun filnavnet."
        ),
    }

    for kolonne, kommentar in kolonne_kommentarer.items():
        kommentar_escaped = kommentar.replace("'", "\\'")
        spark.sql(f"""
            ALTER TABLE {TABELLNAVN} ALTER COLUMN `{kolonne}` COMMENT '{kommentar_escaped}'
        """)

    print(f"✅ Kolonnekommentarer lagt til {len(kolonne_kommentarer)} felt i {TABELLNAVN}.")
else:
    print(f"ℹ️ Tabellen {TABELLNAVN} finnes ikke — kommentarer hoppet over.")


# -----------------------------------------------------------------
# DEL 6: SETT SENSITIVITETS-TAG PÅ BRONSE-TABELLEN
# -----------------------------------------------------------------
# Klassifiserer hele tabellen som "Offentlig", siden valutakurser fra
# Norges Bank er offentlig tilgjengelig informasjon uten personopp-
# lysninger eller forretningssensitivt innhold.
#
# Tags i Unity Catalog er key-value-par satt på katalog/skjema/
# tabell/kolonne-nivå, synlige i Catalog Explorer og via
# information_schema, og kan brukes videre i governance-policyer
# (f.eks. tilgangsstyring basert på tag).
#
# Satt på TABELLNIVÅ (ikke pr. kolonne), siden hele tabellen har samme
# klassifisering. Trygt å kjøre på nytt - SET TAGS overskriver bare
# med samme verdi om den allerede er satt.
# -----------------------------------------------------------------
print("\n" + "=" * 65)
print("DEL 6: Setter sensitivitets-tag på bronse-tabellen")
print("=" * 65)

if spark.catalog.tableExists(TABELLNAVN):
    spark.sql(f"""
        ALTER TABLE {TABELLNAVN} SET TAGS ('sensitivitet' = 'Offentlig')
    """)
    print(f"✅ Tag 'sensitivitet' = 'Offentlig' satt på {TABELLNAVN}.")
else:
    print(f"ℹ️ Tabellen {TABELLNAVN} finnes ikke — tag hoppet over.")


print("\n" + "=" * 65)
print(f"JOBB FULLFØRT — lastetype denne kjøringen: {LASTETYPE.upper()}")
print("=" * 65)
