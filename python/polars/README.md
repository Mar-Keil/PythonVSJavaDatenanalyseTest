# Polars Benchmarks

Dieses Verzeichnis enthaelt Benchmark-Logiken auf Basis der Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Verfuegbare Skripte
- `filter_polars.py`: Filter auf `aircraft_model == "A319neo"`
- `pivot_polars.py`: GroupBy + Summe von `flight_distance` pro `aircraft_model`
- `groupCount_polars.py`: GroupBy + Anzahl pro (`aircraft_model`, `airline_code`)
- `join_polars.py`: Inner Join von `flights` und `airlines` ueber `airline_code`
- `sort_polars.py`: Sortierung nach `flight_number`

## Ausfuehrung (Beispiel)
```bash
python python/polars/filter_polars.py
```

Alle Skripte schreiben ihre Ergebnisse nach `python/polars/out/`:
- `filter_polars.parquet`
- `pivot_polars.parquet`
- `groupCount_polars.parquet`
- `join_polars.parquet`
- `sort_polars.parquet`
