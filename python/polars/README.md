# Polars Dataset Access

Dieses Verzeichnis enthaelt ein minimales Lade-Skript fuer die Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Ausfuehrung
```bash
python python/polars/filter_polars.py
```

## Output
- `python/polars/out/filter.parquet`

## Verwendung in eigenem Code
```python
from python.polars.transformation_polars import load_datasets

flights_df, airlines_df = load_datasets()
```
