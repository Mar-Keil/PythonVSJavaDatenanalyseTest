# Datensatz-Generator

Dieser Ordner nutzt Python, um zwei reproduzierbare Benchmark-Datensaetze als
Parquet-Dateien zu erzeugen (`flights` und `airlines`).

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ./data-gen
```

## Generierung
```bash
python data-gen/generate_parquet.py --rows 1000 --seed 42 --reference-date 2025-12-31
```

Optionen:
- `--rows`: Anzahl Zeilen fuer `flights`
- `--airlines`: Anzahl Airline-Zeilen (Join-Zieltabelle)
- `--seed`: Seed fuer reproduzierbare Daten
- `--reference-date`: Referenzdatum im Format `YYYY-MM-DD`; alle Flugzeiten liegen strikt davor
- `--output-dir`: Zielordner (Default: `out/` im `data-gen`-Ordner)

## Warum `--seed`?
Mit demselben Seed und denselben Parametern werden identische Daten erzeugt.
So bleiben Benchmarks zwischen Pandas, Polars und Tablesaw fair und reproduzierbar.
Ein anderer Seed erzeugt andere, aber gleichartig verteilte Daten.

Erzeugte Dateien:
- `out/flights.parquet`
- `out/airlines.parquet`

Wichtige Spalten in `flights`:
- `flight_number` (eindeutig, praefix `FL`)
- `departure_time` und `arrival_time` (String mit Uhrzeit + Datum)
- `flight_distance` (Int64, Kilometer)

Join-Schluessel:
- `flights.airline_code` = `airlines.airline_code`
