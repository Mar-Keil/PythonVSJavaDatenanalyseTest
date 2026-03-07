# Pandas Benchmarks

Dieses Verzeichnis enthaelt Benchmark-Logiken auf Basis der Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Verfuegbare Skripte
- `filter_pandas.py`: Filter auf `aircraft_model == "A319neo"`
- `pivot_pandas.py`: GroupBy + Summe von `flight_distance` pro `aircraft_model`
- `groupCount_pandas.py`: GroupBy + Anzahl Zeilen pro (`aircraft_model`, `airline_code`)
- `join_pandas.py`: Inner Join von `flights` und `airlines` ueber `airline_code`
- `sort_pandas.py`: Sortierung nach `flight_number`

## Ausfuehrung (Beispiel)
```bash
python python/pandas/filter_pandas.py
```

Alle Skripte schreiben ihre Ergebnisse nach `python/pandas/out/`:
- `filter_pandas.parquet`
- `pivot_pandas.parquet`
- `groupCount_pandas.parquet`
- `join_pandas.parquet`
- `sort_pandas.parquet`
