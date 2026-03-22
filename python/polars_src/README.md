# Polars Benchmarks

Dieses Verzeichnis enthaelt Benchmark-Logiken auf Basis der Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Setup
```bash
python3 -m venv python/.venv
python/.venv/bin/pip install -e ./python
```

Das installiert die benoetigten Python-Abhaengigkeiten inklusive `psutil` fuer
die RAM-Messung und macht `polars_src` als Paket importierbar.

## Benchmark-Ausfuehrung
```bash
python/.venv/bin/python -m polars_src.benchmarking.run_benchmarks
```

Alternativ nach Installation:
```bash
python/.venv/bin/polars-benchmarks
```

Die Benchmark-Ergebnisse werden nach `python/polars_src/out/results.csv` geschrieben.
