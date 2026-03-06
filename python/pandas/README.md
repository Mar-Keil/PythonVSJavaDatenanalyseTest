# Pandas Dataset Access

Dieses Verzeichnis enthaelt ein minimales Lade-Skript fuer die Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Ausfuehrung
```bash
python python/pandas/transformation_pandas.py
```

## Output
- `python/pandas/out/filter.parquet`

## Verwendung in eigenem Code
```python
from python.pandas.transformation_pandas import load_datasets

flights_df, airlines_df = load_datasets()
```
