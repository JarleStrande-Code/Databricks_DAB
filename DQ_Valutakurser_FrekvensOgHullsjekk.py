# Databricks notebook source
# =================================================================
# DATAKVALITET: Frekvens- og hull-sjekk av dlt_bronse.valutakurs
# MED SJEKK MOT NORGES BANK PUBLISERINGSKALENDER
# =================================================================
# Denne notebooken sjekker datakvaliteten i dlt_bronse-tabellen
# test_dab.dlt_bronse.valutakurs og logger eventuelle avvik til en
# egen DQ-tabell (test_dab.dlt_bronse.valutakurs_dq).
#
# TO SJEKKER KJØRES:
#
#   SJEKK 1 (FREKVENS):  Verifiserer at antall kursnoteringer per
#                         uke per valuta stemmer med antall forventede
#                         publiseringsdager fra kalender-tabellen.
#                         Kalenderen filtrerer bort helger og kjente
#                         ikke-publiseringsdager.
#
#   SJEKK 2 (HULL):      Finner konkrete datoer som mangler for hver
#                         valuta ved å sammenligne alle faktiske datoer
#                         mot alle forventede publiseringsdager i perioden
#                         tabellen dekker. Forventede datoer hentes fra
#                         kalender-tabellen for Norges Bank.
#
# RESULTAT:
#   - Avvik skrives til test_dab.dlt_bronse.valutakurs_dq (Delta-tabell)
#   - Jobben fortsetter uansett (ingen exception kastes)
#   - Kjøres ideelt sett etter hovedjobben (valutakurs_full_jobb_merge)
#     i samme Databricks Workflow, som et eget Task
#
# KALENDER:
#   Notebooken bruker en egen Delta-tabell som angir hvilke datoer
#   Norges Bank forventes å publisere valutakurser. Dette gjør at
#   helligdager og andre ikke-publiseringsdager ikke feilaktig blir
#   logget som hull.
# =================================================================


# -----------------------------------------------------------------
# DEL 0: IMPORTER
# -----------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType
)
from datetime import datetime, date


# -----------------------------------------------------------------
# DEL 1: KONFIGURASJON
# -----------------------------------------------------------------
KATALOG       = "test_dab"
BRONSE_SKJEMA = "bronse"                                              # ← endret
KILDE_TABELL  = f"{KATALOG}.{BRONSE_SKJEMA}.valutakurs"
DQ_TABELL     = f"{KATALOG}.{BRONSE_SKJEMA}.valutakurs_dq"
KALENDER_TABELL = f"{KATALOG}.{BRONSE_SKJEMA}.norges_bank_valutakurs_kalender"

# Tidspunkt sjekken kjøres - brukes som sporingskolonne i DQ-tabellen
SJEKK_TIDSPUNKT = datetime.now()

print("=" * 65)
print("DATAKVALITETSSJEKK: valutakurs")
print("=" * 65)
print(f"Kilde:          {KILDE_TABELL}")
print(f"DQ-tabell:      {DQ_TABELL}")
print(f"Kalender:       {KALENDER_TABELL}")
print(f"Sjekk kjørt:    {SJEKK_TIDSPUNKT.strftime('%Y-%m-%d %H:%M:%S')}")


# -----------------------------------------------------------------
# DEL 2: SIKRE AT DQ-TABELLEN FINNES
# -----------------------------------------------------------------
# DQ-tabellen opprettes automatisk ved første kjøring hvis den ikke
# finnes fra før. Skjemaet er fast og endres ikke over tid, siden
# alle sjekk-typer bruker samme kolonnestruktur.
# -----------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DQ_TABELL} (
        sjekk_type          STRING    COMMENT 'Type DQ-sjekk: FREKVENS eller HULL',
        valuta              STRING    COMMENT 'Valutakoden avviket gjelder for',
        periode             STRING    COMMENT 'Uke (FREKVENS) eller konkret dato (HULL) avviket gjelder for',
        forventet           INT       COMMENT 'Forventet antall rader (FREKVENS) eller 1 = dato forventes å finnes (HULL)',
        faktisk             INT       COMMENT 'Faktisk antall rader funnet i tabellen',
        avvik               INT       COMMENT 'Differansen mellom forventet og faktisk (forventet - faktisk)',
        beskrivelse         STRING    COMMENT 'Lesbar forklaring på avviket',
        sjekk_tidspunkt     TIMESTAMP COMMENT 'Når denne DQ-sjekken ble kjørt'
    )
    USING DELTA
    COMMENT 'Datakvalitetslogg for dlt_bronse.valutakurs. Inneholder avvik fra frekvens- og hull-sjekker.'
""")

print("\n✅ DQ-tabell verifisert/opprettet.")


# -----------------------------------------------------------------
# DEL 3: LAST KILDETABELLEN OG KALENDER
# -----------------------------------------------------------------
df = spark.table(KILDE_TABELL)
kalender_df = spark.table(KALENDER_TABELL)

# Hent første og siste dato i tabellen - brukes som grense for
# hvilken periode vi genererer forventede hverdager for
periode = df.agg(
    F.min("dato").alias("tidligste_dato"),
    F.max("dato").alias("seneste_dato")
).collect()[0]

TIDLIGSTE_DATO = periode["tidligste_dato"]
SENESTE_DATO   = periode["seneste_dato"]
VALUTAER       = [r["valuta"] for r in df.select("valuta").distinct().collect()]

print(f"\nPeriode i tabellen: {TIDLIGSTE_DATO} til {SENESTE_DATO}")
print(f"Valutaer funnet:    {', '.join(VALUTAER)}")

# Hent forventede publiseringsdager fra kalenderen for samme periode
# som finnes i kildetabellen. Dette erstatter tidligere logikk der alle
# mandag-fredag ble regnet som forventede kursdager.
forventede_df = (kalender_df
    .filter(F.col("dato").between(F.lit(TIDLIGSTE_DATO), F.lit(SENESTE_DATO)))
    .filter(F.col("forventet_publisering") == True)
    .select(F.col("dato").cast("date").alias("dato"))
    .distinct()
)

antall_forventede_publiseringsdager = forventede_df.count()
print(f"Forventede publiseringsdager fra kalender: {antall_forventede_publiseringsdager}")

if antall_forventede_publiseringsdager == 0:
    raise ValueError(
        f"Fant ingen forventede publiseringsdager i {KALENDER_TABELL} "
        f"for perioden {TIDLIGSTE_DATO} til {SENESTE_DATO}."
    )


# ===================================================================
# SJEKK 1: FREKVENSSJEKK
# ===================================================================
# For hver valuta og uke: tell antall kursnoteringer og sammenlign
# med antall forventede publiseringsdager i kalenderen.
#
# Eksempel: en uke med 5 forventede publiseringsdager skal ha 5 rader
# per valuta. Hvis kalenderen sier at kun 4 dager forventes publisert
# på grunn av helligdag, forventes kun 4 rader per valuta.
# ===================================================================
print("\n" + "=" * 65)
print("SJEKK 1: FREKVENS (forventede publiseringsdager per uke per valuta)")
print("=" * 65)

# Tell forventede publiseringsdager per uke basert på kalender-tabellen
forventede_per_uke = (forventede_df
    .withColumn("aar_uke", F.concat(
        F.expr("year(dato)").cast("string"),
        F.lit("-"),
        F.lpad(F.weekofyear("dato").cast("string"), 2, "0")
    ))
    .groupBy("aar_uke")
    .agg(F.count("*").alias("forventede_publiseringsdager"))
)

# Tell faktiske rader per valuta per uke
faktiske_per_uke = (df
    .withColumn("aar_uke", F.concat(
        F.expr("year(dato)").cast("string"),
        F.lit("-"),
        F.lpad(F.weekofyear("dato").cast("string"), 2, "0")
    ))
    .groupBy("valuta", "aar_uke")
    .agg(F.count("*").alias("faktiske_rader"))
)

# Krysskombiner alle valutaer med alle uker for å fange manglende uker
alle_kombinasjoner = (forventede_per_uke
    .crossJoin(spark.createDataFrame(
        [(v,) for v in VALUTAER], schema=["valuta"]
    ))
)

# Join og finn avvik
frekvens_avvik = (alle_kombinasjoner
    .join(faktiske_per_uke, ["valuta", "aar_uke"], "left")
    .withColumn("faktiske_rader", F.coalesce(F.col("faktiske_rader"), F.lit(0)))
    .withColumn("avvik", F.col("forventede_publiseringsdager") - F.col("faktiske_rader"))
    .filter(F.col("avvik") != 0)
)

antall_frekvens_avvik = frekvens_avvik.count()
print(f"Frekvensavvik funnet: {antall_frekvens_avvik}")

if antall_frekvens_avvik > 0:
    frekvens_avvik.select(
        "valuta", "aar_uke", "forventede_publiseringsdager", "faktiske_rader", "avvik"
    ).orderBy("valuta", "aar_uke").show(20, truncate=False)


# ===================================================================
# SJEKK 2: HULL-SJEKK
# ===================================================================
# For hver valuta: finn konkrete datoer som er forventet publisert
# ifølge kalenderen, men som ikke finnes i tabellen.
#
# Fremgangsmåte:
#   1. Hent forventede publiseringsdager fra kalender-tabellen
#   2. Krysskombiner med alle valutaer
#   3. LEFT JOIN mot faktiske data
#   4. Rader uten match er hull
# ===================================================================
print("\n" + "=" * 65)
print("SJEKK 2: HULL (manglende publiseringsdatoer per valuta)")
print("=" * 65)

# Krysskombiner forventede publiseringsdager med alle valutaer
forventede_kombinasjoner = (forventede_df
    .crossJoin(spark.createDataFrame(
        [(v,) for v in VALUTAER], schema=["valuta"]
    ))
)

# Finn hull — left_anti returnerer kun rader som IKKE fant match
hull_df = (forventede_kombinasjoner
    .join(
        df.select("valuta", "dato"),
        ["valuta", "dato"],
        "left_anti"
    )
    .withColumn("avvik", F.lit(1))
    .orderBy("valuta", "dato")
)

antall_hull = hull_df.count()
print(f"Hull funnet: {antall_hull}")

if antall_hull > 0:
    hull_df.show(20, truncate=False)
    print("\nNB: Hullene er nå vurdert mot kalender-tabellen, slik at datoer")
    print("    der forventet_publisering = false ikke skal vises som avvik.")


# ===================================================================
# DEL 4: SKRIV AVVIK TIL DQ-TABELLEN
# ===================================================================
# Alle avvik fra begge sjekker samles i én DataFrame og skrives til
# DQ-tabellen via MERGE INTO på (sjekk_type, valuta, periode,
# sjekk_tidspunkt), slik at gjentatte kjøringer ikke lager
# duplikate rader for samme sjekk-tidspunkt.
# ===================================================================
print("\n" + "=" * 65)
print("DEL 4: Skriver avvik til DQ-tabellen")
print("=" * 65)

from delta.tables import DeltaTable

avvik_rader = []

# --- Samle frekvensavvik ---
for rad in frekvens_avvik.collect():
    avvik_rader.append((
        "FREKVENS",
        rad["valuta"],
        rad["aar_uke"],
        int(rad["forventede_publiseringsdager"]),
        int(rad["faktiske_rader"]),
        int(rad["avvik"]),
        (f"Uke {rad['aar_uke']}: forventet {rad['forventede_publiseringsdager']} "
         f"kursnoteringer for {rad['valuta']}, fant {rad['faktiske_rader']}. "
         f"Avvik: {rad['avvik']} manglende dag(er)."),
        SJEKK_TIDSPUNKT,
    ))

# --- Samle hull-avvik ---
for rad in hull_df.collect():
    avvik_rader.append((
        "HULL",
        rad["valuta"],
        str(rad["dato"]),
        1,
        0,
        1,
        (f"Manglende kursnotering for {rad['valuta']} "
         f"på forventet publiseringsdag {rad['dato']} "
         f"i henhold til kalenderen {KALENDER_TABELL}."),
        SJEKK_TIDSPUNKT,
    ))

if avvik_rader:
    avvik_schema = StructType([
        StructField("sjekk_type",      StringType(),    False),
        StructField("valuta",          StringType(),    False),
        StructField("periode",         StringType(),    False),
        StructField("forventet",       IntegerType(),   True),
        StructField("faktisk",         IntegerType(),   True),
        StructField("avvik",           IntegerType(),   True),
        StructField("beskrivelse",     StringType(),    True),
        StructField("sjekk_tidspunkt", TimestampType(), False),
    ])

    avvik_df = spark.createDataFrame(avvik_rader, schema=avvik_schema)

    # MERGE på (sjekk_type, valuta, periode, sjekk_tidspunkt) for å
    # unngå duplikater ved gjentatte kjøringer i samme tidspunkt-vindu
    dq_tabell = DeltaTable.forName(spark, DQ_TABELL)
    (dq_tabell.alias("mål")
        .merge(
            avvik_df.alias("kilde"),
            """`mål`.sjekk_type      = kilde.sjekk_type
            AND `mål`.valuta          = kilde.valuta
            AND `mål`.periode         = kilde.periode
            AND `mål`.sjekk_tidspunkt = kilde.sjekk_tidspunkt"""
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ {len(avvik_rader)} avvik skrevet til {DQ_TABELL}.")
else:
    print("✅ Ingen avvik funnet — ingen rader skrevet til DQ-tabellen.")


# -----------------------------------------------------------------
# DEL 5: OPPSUMMERING
# -----------------------------------------------------------------
print("\n" + "=" * 65)
print("OPPSUMMERING")
print("=" * 65)
print(f"Periode sjekket:       {TIDLIGSTE_DATO} til {SENESTE_DATO}")
print(f"Kalender brukt:        {KALENDER_TABELL}")
print(f"Publiseringsdager:     {antall_forventede_publiseringsdager}")
print(f"Valutaer:              {', '.join(VALUTAER)}")
print(f"Frekvensavvik funnet:  {antall_frekvens_avvik}")
print(f"Hull funnet:           {antall_hull}")
print(f"Totalt avvik logget:   {len(avvik_rader)}")

if len(avvik_rader) > 0:
    print(f"\n⚠️ Avvik er logget til {DQ_TABELL}.")
    print("   Spørr mot tabellen for å se detaljer:")
    print(f"   SELECT * FROM {DQ_TABELL}")
    print(f"   WHERE DATE(sjekk_tidspunkt) = '{date.today()}'")
    print(f"   ORDER BY sjekk_type, valuta, periode;")
else:
    print("\n✅ Alle sjekker passerte uten avvik.")

print("\n" + "=" * 65)
print("DATAKVALITETSSJEKK FULLFØRT")
print("=" * 65)