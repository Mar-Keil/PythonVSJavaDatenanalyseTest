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
die RAM-Messung und macht `benchmarking` und `polars_logic` als Pakete importierbar.

## Benchmark-Ausfuehrung
```bash
python/.venv/bin/python -m benchmarking.run_polars_benchmarks
```

Alternativ nach Installation:
```bash
python/.venv/bin/polars-benchmarks
```

Die Benchmark-Ergebnisse werden nach `python/benchmarking/out_polars/*.csv` geschrieben.
