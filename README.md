# PythonVSJavaDatenanalyseTest
Das Projekt vergleicht die Effizienz von Datenanalyse-Frameworks in Java und Python.
Geplant sind Analysen mit Polars, Pandas und Tablesaw, inkl. Messung von Laufzeit,
CPU und RAM.

## Ordnerstruktur
- `java/`: Maven-Projekt (OpenJDK 25) für Tablesaw
- `python/`: Python-Projekt für Polars/Pandas
- `data-gen/`: Python-Generator fuer gemeinsame Parquet-Eingabedaten
- `latex/`: Lokale LaTeX-Dokumentation (nicht im Git)

## Zielarchitektur (geplant)
- Eine gemeinsame Datenbasis fuer beide Sprachen (Parquet-Dateien als kanonisches Format).
- Datensatz wird reproduzierbar erzeugt (fester Seed).
- Beide Implementierungen arbeiten auf exakt denselben Eingangsdaten.
- Framework-native Analysen ohne DuckDB, damit die Libraries direkt verglichen werden.

## Benchmark-Fokus
Die zentrale Frage ist: Welche Sprache/Library ist fuer eine konkrete Aufgabe am effizientesten?
Daher werden dieselben Aufgaben direkt in Pandas, Polars und Tablesaw implementiert
(gleiche Eingabedaten, gleiche Zielmetriken, gleiche Ergebnisvalidierung).

Beispiel-Aufgaben:
- Aggregation (z. B. Umsatz pro Region)
- Filter + Sortierung (Top-N)
- Join ueber mehrere Tabellen
- GroupBy mit mehreren Kennzahlen

## Naechste Schritte
1. Datenschema und Datengenerator in `data-gen/` definieren (reproduzierbar per Seed).
2. Parquet-Daten fuer mehrere Groessenklassen erzeugen (z. B. S/M/L).
3. Referenz-Aufgaben als Spezifikation festlegen (Input, erwartetes Ergebnis, Toleranz).
4. Aufgaben in Pandas, Polars und Tablesaw implementieren.
5. Benchmark-Harness fuer Zeit/CPU/RAM plus Ergebnisvalidierung aufbauen.

## Hinweise
- Java laeuft ueber `mvn -f java/pom.xml package`.
- Python-Abhaengigkeiten sollen in `python/pyproject.toml` definiert werden.
- Data-Generator-Abhaengigkeiten sind in `data-gen/pyproject.toml` definiert.
- Datensatz-Generierung: `python data-gen/generate_parquet.py --rows 1000000 --seed 42`
