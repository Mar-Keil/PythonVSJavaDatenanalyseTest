import csv
import math
import os
import sys
import tempfile

NORMALIZED_COUNTER_UNITS = {
    ":CPU": "ratio/iter",
}


def parse_decimal(value: str) -> float:
    return float(value.replace(",", "."))


def format_decimal(value: float) -> str:
    return f"{value:.6f}".replace(".", ",")


def main() -> None:
    csv_path = sys.argv[1]

    with open(csv_path, newline="", encoding="utf-8") as file:
        rows = list(csv.reader(file))

    if not rows:
        return

    header = rows[0]
    benchmark_idx = header.index("Benchmark")
    samples_idx = header.index("Samples")
    score_idx = header.index("Score")
    unit_idx = header.index("Unit")

    normalized_rows = [header]

    for row in rows[1:]:
        benchmark = row[benchmark_idx]
        score_raw = row[score_idx]

        if score_raw == "NaN":
            normalized_rows.append(row)
            continue

        try:
            score = parse_decimal(score_raw)
        except ValueError:
            normalized_rows.append(row)
            continue

        matching_suffix = next(
            (suffix for suffix in NORMALIZED_COUNTER_UNITS if benchmark.endswith(suffix)),
            None,
        )
        if matching_suffix is not None:
            samples_raw = row[samples_idx]
            try:
                samples = float(samples_raw)
            except ValueError:
                normalized_rows.append(row)
                continue

            if math.isfinite(score) and samples > 0:
                row[score_idx] = format_decimal(score / samples)

            row[unit_idx] = NORMALIZED_COUNTER_UNITS[matching_suffix]

        normalized_rows.append(row)

    fd, tmp_path = tempfile.mkstemp(
        prefix="jmh_results_",
        suffix=".csv",
        dir=os.path.dirname(csv_path),
    )
    os.close(fd)

    with open(tmp_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(normalized_rows)

    os.replace(tmp_path, csv_path)


if __name__ == "__main__":
    main()
