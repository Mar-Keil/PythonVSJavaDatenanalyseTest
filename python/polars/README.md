# Polars Benchmarks

Dieses Verzeichnis enthaelt Benchmark-Logiken auf Basis der Datensaetze aus
`data-gen/out`.

## Eingabedaten
- `data-gen/out/flights.parquet`
- `data-gen/out/airlines.parquet`

## Verfuegbares Skript
- `logic.py filter`: Filter auf `aircraft_model == "A319neo"`
- `logic.py pivot`: GroupBy + Summe von `flight_distance` pro `aircraft_model`
- `logic.py groupCount`: GroupBy + Anzahl pro (`aircraft_model`, `airline_code`)
- `logic.py join`: Inner Join von `flights` und `airlines` ueber `airline_code`
- `logic.py sort`: Sortierung nach `flight_number`
- `logic.py`: Fuehrt standardmaessig alle Transformationen aus

## Ausfuehrung (Beispiel)
```bash
python python/polars/logic.py filter
```

Das Skript schreibt seine Ergebnisse nach `python/polars/out/`:
- `filter_polars.parquet`
- `pivot_polars.parquet`
- `groupCount_polars.parquet`
- `join_polars.parquet`
- `sort_polars.parquet`
