# Datensatz-Generator

Dieser Ordner nutzt Python, um zwei reproduzierbare Benchmark-Datensaetze als
Parquet-Dateien zu erzeugen (`aircraft` und `airlines`).

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ./data-gen
```

## Generierung
```bash
python data-gen/generate_parquet.py --rows 1000 --seed 42
```

Optionen:
- `--rows`: Anzahl Zeilen fuer `aircraft`
- `--airlines`: Anzahl Airline-Zeilen (Join-Zieltabelle)
- `--seed`: Seed fuer reproduzierbare Daten
- `--output-dir`: Zielordner (Default: `out/` im `data-gen`-Ordner)

## Warum `--seed`?
Mit demselben Seed und denselben Parametern werden identische Daten erzeugt.
So bleiben Benchmarks zwischen Pandas, Polars und Tablesaw fair und reproduzierbar.
Ein anderer Seed erzeugt andere, aber gleichartig verteilte Daten.

Erzeugte Dateien:
- `out/aircraft.parquet`
- `out/airlines.parquet`

`flights` enthaelt zusaetzlich die Spalte:
- `flight_distance` (Float64, Kilometer)

Join-Schluessel:
- `aircraft.airline_code` = `airlines.airline_code`
