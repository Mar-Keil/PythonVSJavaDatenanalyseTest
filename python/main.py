def main() -> None:
    import pandas as pd
    import polars as pl

    print("Python benchmark test run")
    print(f"pandas: {pd.__version__}")
    print(f"polars: {pl.__version__}")


if __name__ == "__main__":
    main()
