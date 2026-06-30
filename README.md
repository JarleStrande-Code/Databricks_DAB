# Databricks_DAB
# Valutakurser — Norges Bank til Databricks

En liten datapipeline som henter daglige valutakurser fra Norges Banks API og laster dem inn i en Delta-tabell i Databricks, klar for videre bruk i rapporter og analyser.

## Hva gjør pipelinen?

Pipelinen består av ett samlet Databricks-notebook som kjører fire steg i sekvens:

1. **Extract** — henter valutakurser fra [Norges Banks SDMX-API](https://data.norges-bank.no) og lagrer rå JSON i en landing-sone.
2. **Autoloader** — leser nye JSON-filer fra landing-sonen automatisk, sporet via et eget checkpoint.
3. **Flatening** — gjør om den nøstede SDMX-strukturen til enkle kolonner: `valuta`, `dato`, `kurs`.
4. **MERGE** — setter inn kun nye valuta+dato-kombinasjoner i bronse-tabellen. Eksisterende rader overskrives aldri.

```
Norges Bank API → Landing-sone (rå JSON) → Autoloader → Bronse-tabell (Delta)
```

## Tabell

| | |
|---|---|
| Katalog | `test_dab` |
| Tabell | `test_dab.bronse.valutakurs` |
| Format | Delta Lake |
| Sensitivitet | Offentlig |

### Kolonner

| Kolonne | Beskrivelse |
|---|---|
| `valuta` | Valutakode, f.eks. USD, EUR eller SEK |
| `dato` | Datoen kursen gjelder for |
| `kurs` | Hvor mange norske kroner (NOK) valutaen er verdt på denne datoen |
| `HentetTidspunkt` | Når dataene ble hentet fra Norges Bank |
| `LastetDato` | Når raden ble lagt inn i tabellen. Endres aldri senere |
| `_kildefil` | Hvilken fil i landing-sonen raden kommer fra (full filsti) |
| `_kildefil_navn` | Samme som `_kildefil`, men kun filnavnet |

## Hvorfor ingen duplikater?

Hver rad er unikt identifisert av kombinasjonen `valuta` + `dato`. Innlasting skjer via `MERGE INTO` med kun en `WHEN NOT MATCHED`-gren — finnes raden fra før, gjøres ingenting; finnes den ikke, settes den inn med en fersk `LastetDato`. Dette gjør at jobben er trygg å kjøre flere ganger uten å risikere dobbel data.

## Last og parametere

Jobben styres av fire parametere, som kan settes som **Job Parameters** i Databricks Workflows uten å endre koden:

| Parameter | Standardverdi | Beskrivelse |
|---|---|---|
| `lastetype` | `auto` | `auto` (full ved tom tabell, ellers inkrementell), `full`, eller `inkrementell` |
| `valutaer` | `EUR,SEK` | Kommaseparert liste over valutaer som hentes |
| `full_start_periode` | `2026-01-01` | Startdato for full last |
| `katalog` | `test_dab` | Hvilken Unity Catalog-katalog jobben kjører mot |

## Kjøring

- **Schedule**: daglig, kl. 07:00 (Europe/Oslo)
- **Trigger**: `trigger(availableNow=True)` — prosesserer alle ulest filer og stopper
- **Engangskjøring med andre parametere**: bruk "Run now with different parameters" i Databricks, uten å endre jobbens lagrede standardverdier

## Mappestruktur (Unity Catalog)

```
test_dab
├── landing
│   ├── valuta          (volum - rå JSON-filer fra Norges Bank)
│   └── _checkpoints     (volum - Autoloader sine checkpoints)
└── bronse
    └── valutakurs       (Delta-tabell)
```

## Robusthet

- **Idempotent oppsett**: `CREATE SCHEMA/VOLUME IF NOT EXISTS` gjør at jobben kan kjøres i et helt nytt miljø uten manuelt forarbeid.
- **Skjema-evolusjon i to lag**: `cloudFiles.schemaEvolutionMode` håndterer nye felt i kilde-JSON, `mergeSchema` håndterer nye kolonner ved skriving til bronse.
- **Kolonnekommentarer og sensitivitetstag** settes automatisk i Unity Catalog ved hver kjøring, slik at tabellen er selvdokumenterende.

## Kjente begrensninger / videre arbeid

- Ingen automatisk varsling ved feilet jobb ennå.
- Ingen datakvalitetssjekk (f.eks. hull i dato-serien, urimelige kursverdier).
- Ingen silver-lag med beregnede felt (dag-til-dag-endring, prosentvis endring) — finnes foreløpig kun som ad-hoc SQL-spørringer.

## Lisens / kontakt

Internt dataprosjekt. Kontakt teamet for spørsmål.
