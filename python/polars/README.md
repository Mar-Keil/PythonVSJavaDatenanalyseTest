# Polars Dataset Access

Dieses Verzeichnis enthaelt ein minimales Lade-Skript fuer die Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Ausfuehrung
```bash
python python/polars/load_datasets.py
```

## Verwendung in eigenem Code
```python
from python.polars.load_datasets import load_datasets

flights_df, airlines_df = load_datasets()
```
