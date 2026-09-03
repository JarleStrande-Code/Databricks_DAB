# Databricks notebook source
# =================================================================
# DLT-PIPELINE: Valutakurser — Landing → dlt_bronse.valutakurs
# =================================================================
# Dette er kildekoden til selve Lakeflow Declarative Pipeline
# (DLT-pipelinen). Den erstatter STEG 2-4 i den gamle notebooken
# (Autoloader-lesing, flatening og MERGE).
#
# STEG 1 (EXTRACT mot Norges Banks API) er IKKE med her - DLT-
# pipelines skal ikke gjøre eksterne REST-kall inni pipeline-grafen,
# siden det bryter med den deklarative/inkrementelle kjøremodellen.
# Ekstraksjonen kjøres som et eget forutgående steg i en Databricks
# Job (se extract_valutakurser_notebook.py), som skriver rå JSON til
# landing-sonen slik denne pipelinen forventer.
#
# TARGET-SCHEMA: satt til 'dlt_bronse' i pipeline-innstillingene
# (Catalog = f.eks. test_dab, Target schema = dlt_bronse), IKKE i
# koden her - tabellnavn under er derfor UKVALIFISERTE.
#
# DESIGN (tre lag i denne pipelinen):
#   _valutakurser_raw     -> (temporary/internal) Auto Loader leser
#                             rå JSON-tekstfiler fra landing-sonen.
#   _valutakurser_flatet  -> (temporary/internal) SDMX-strukturen
#                             flates ut til én rad pr. valuta+dato,
#                             parset med en UDF (kjører på executors,
#                             IKKE collect() til driver som i
#                             original-koden).
#   valutakurs            -> (PUBLISERT bronse-tabell) mottar kun
#                             NYE valuta+dato-kombinasjoner via
#                             dlt.apply_changes (AUTO CDC / SCD1).
#
# De to første er markert temporary=True, så de aldri publiseres som
# egne tabeller i dlt_bronse - akkurat som i original-jobben, der
# kun den utflatede tabellen 'valutakurs' var synlig, ikke rå JSON.
#
# OBS - forskjell fra original MERGE-logikk:
# Original brukte whenNotMatchedInsertAll UTEN whenMatchedUpdate,
# altså strengt "sett inn nye, rør aldri eksisterende rader".
# dlt.apply_changes med stored_as_scd_type=1 vil derimot OPPDATERE
# en eksisterende (valuta, dato)-rad dersom en ny hendelse med
# samme nøkkel skulle dukke opp med en annen kurs-verdi (f.eks. ved
# korrigert historikk fra Norges Bank). I praksis endres ikke en
# offisiell dagskurs i ettertid, så forskjellen har liten praktisk
# betydning - men den er verdt å være klar over.
# =================================================================

import json
import re
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, DoubleType
)


# -----------------------------------------------------------------
# KONFIGURASJON
# -----------------------------------------------------------------
# Landing-stien settes IKKE hardkodet her, men som en pipeline-
# konfigurasjonsverdi ("Configuration" i pipeline-innstillingene),
# slik at samme kode kan peke mot ulike miljøer/kataloger uten
# kodeendring - tilsvarende widget-parameteren 'katalog' i original-
# jobben.
#
# Sett i pipeline-innstillingene (JSON):
#   "configuration": {
#     "valutakurs.landing_path": "/Volumes/test_dab/landing/valuta"
#   }
# -----------------------------------------------------------------
LANDING_PATH = spark.conf.get(
    "valutakurs.landing_path", "/Volumes/test_dab/landing/valuta"
)


# -----------------------------------------------------------------
# LAG 1: Rå innlesing av JSON-filer (tilsvarer original STEG 2)
# -----------------------------------------------------------------
@dlt.table(
    name="_valutakurser_raw",
    comment=(
        "Internt lag: rå JSON-respons fra Norges Bank, én rad pr. fil, "
        "lest inn med Auto Loader. Publiseres ikke i dlt_bronse."
    ),
    temporary=True,
)
def valutakurser_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .load(LANDING_PATH)
        .withColumn("_kildefil", F.col("_metadata.file_path"))
    )


# -----------------------------------------------------------------
# LAG 2: Flatening av SDMX-strukturen (tilsvarer original STEG 3)
# -----------------------------------------------------------------
# Samme parse-logikk som i original-koden, men flyttet inn i en UDF
# slik at parsingen kjører fordelt på executors pr. rad/fil, i
# stedet for å collect()-e hele batchen til driveren først.
# -----------------------------------------------------------------
_SDMX_SCHEMA = ArrayType(StructType([
    StructField("valuta", StringType()),
    StructField("dato", StringType()),
    StructField("kurs", DoubleType()),
]))


def _parse_sdmx(json_tekst):
    """Parser én SDMX-JSON-respons til en liste av (valuta, dato, kurs).

    Returnerer tom liste ved parse-feil - raden forsvinner da naturlig
    ved explode() lenger ned, tilsvarende try/except-hoppet-over-fila
    i original-koden (men uten den samme fil-spesifikke logg-linjen,
    siden UDF-er kjører på executors og ikke bør skrive til driver-
    loggen for hver rad).
    """
    try:
        respons = json.loads(json_tekst)["data"]
        dataset = respons["dataSets"][0]
        struktur = respons["structure"]

        serie_dimensjoner = struktur["dimensions"]["series"]
        valuta_dim_indeks = next(
            i for i, d in enumerate(serie_dimensjoner) if d["id"] == "BASE_CUR"
        )
        valuta_verdier = serie_dimensjoner[valuta_dim_indeks]["values"]

        obs_dimensjoner = struktur["dimensions"]["observation"]
        tid_verdier = obs_dimensjoner[0]["values"]

        rader = []
        for serie_nokkel, serie_innhold in dataset["series"].items():
            indekser = [int(x) for x in serie_nokkel.split(":")]
            valuta_kode = valuta_verdier[indekser[valuta_dim_indeks]]["id"]

            for tid_indeks_str, observasjon in serie_innhold["observations"].items():
                tid_indeks = int(tid_indeks_str)
                dato_str = tid_verdier[tid_indeks]["id"]
                kurs_verdi = float(observasjon[0])
                rader.append((valuta_kode, dato_str, kurs_verdi))

        return rader

    except (KeyError, IndexError, StopIteration, ValueError, TypeError):
        return []


parse_sdmx_udf = F.udf(_parse_sdmx, _SDMX_SCHEMA)

# Regex for å hente ut kjøretidspunktet fra filnavnet, f.eks.
# valutakurser_full_2026-08-20_20260820_101500.json -> 20260820_101500
_TIDSSTEMPEL_REGEX = r"_(\d{8}_\d{6})\.json$"


@dlt.table(
    name="_valutakurser_flatet",
    comment=(
        "Internt lag: utflatet SDMX-struktur, én rad pr. valuta+dato-"
        "observasjon, før dedup/merge mot bronse-tabellen."
    ),
    temporary=True,
)
@dlt.expect_or_drop("gyldig_kurs", "kurs IS NOT NULL AND kurs > 0")
@dlt.expect_or_drop("gyldig_dato", "dato IS NOT NULL")
@dlt.expect_or_drop("gyldig_valuta", "valuta IS NOT NULL")
def valutakurser_flatet():
    raa = dlt.read_stream("_valutakurser_raw")

    return (
        raa
        .withColumn("observasjoner", parse_sdmx_udf(F.col("value")))
        .withColumn("observasjon", F.explode("observasjoner"))
        .select(
            F.col("observasjon.valuta").alias("valuta"),
            F.to_date(F.col("observasjon.dato")).alias("dato"),
            F.col("observasjon.kurs").alias("kurs"),
            F.to_timestamp(
                F.regexp_extract(F.col("_kildefil"), _TIDSSTEMPEL_REGEX, 1),
                "yyyyMMdd_HHmmss",
            ).alias("HentetTidspunkt"),
            F.current_timestamp().alias("LastetDato"),
            F.col("_kildefil"),
        )
    )


# -----------------------------------------------------------------
# LAG 3: MERGE mot bronse-tabellen (tilsvarer original STEG 4)
# -----------------------------------------------------------------
# dlt.create_streaming_table oppretter mål-tabellen 'valutakurs' i
# pipelinens target-schema (dlt_bronse, satt i pipeline-innstillinger
# - IKKE her i koden).
#
# dlt.apply_changes med stored_as_scd_type=1 og keys=["valuta","dato"]
# gir samme kjerneoppførsel som original sin MERGE INTO ... WHEN NOT
# MATCHED INSERT ALL: kun nye valuta+dato-kombinasjoner settes inn.
# LastetDato settes i LAG 2, FØR raden når dette steget, og endres
# derfor aldri i ettertid for en rad som allerede finnes - i tråd med
# kommentaren i original-jobben.
# -----------------------------------------------------------------
dlt.create_streaming_table(
    name="valutakurs",
    comment=(
        "Bronse: én rad pr. valuta+dato-observasjon fra Norges Bank. "
        "Kun nye kombinasjoner settes inn via AUTO CDC/MERGE."
    ),
    table_properties={
        "quality": "bronze",
        "sensitivitet": "Offentlig",
    },
)

dlt.apply_changes(
    target="valutakurs",
    source="_valutakurser_flatet",
    keys=["valuta", "dato"],
    sequence_by=F.col("LastetDato"),
    stored_as_scd_type=1,
)
