# PythonVSJavaDatenanalyseTest
Das Projekt vergleicht die Effizienz von Datenanalyse-Frameworks in Java und Python.
Geplant sind Analysen mit Polars, Pandas, DuckDB und Tablesaw, inkl. Messung von Laufzeit,
CPU und RAM.

## Ordnerstruktur
- `java/`: Maven-Projekt (OpenJDK 25) fuer Java-Analysen
- `python/`: Python-Projekt fuer Polars/Pandas/DuckDB
- `data-gen/`: Platzhalter fuer die Datensatz-Erzeugung
- `latex/`: Lokale LaTeX-Dokumentation (nicht im Git)

## Hinweise
- Java laeuft ueber `mvn -f java/pom.xml package`.
- Python-Abhaengigkeiten sind in `python/pyproject.toml` definiert.
