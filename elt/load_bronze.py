
from pathlib import Path

from snowflake_io import (
    get_snowflake_connection,
    load_file,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LANDING_DIR = PROJECT_ROOT / "elt" / "landing"

SOURCES = {
    "web": "ORDERS_RAW",
    "mobile": "ORDERS_RAW",
    "crm": "CUSTOMERS_RAW",
    "fx": "FX_RAW",
}


def main():
    connection = get_snowflake_connection()

    try:
        total_files = 0

        for source, target in SOURCES.items():

            source_dir = LANDING_DIR / source

            files = sorted(
                source_dir.glob("*/*.json.gz")
            )

            if not files:
                print(f"\nNo files found for {source}")
                continue

            for file_path in files:

                print(
                    f"\nSource: {source} | Target: {target}"
                )

                load_file(
                    connection,
                    file_path,
                    source
                )

                total_files += 1

        print(
            f"\nBronze loading completed. "
            f"Files processed: {total_files}"
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
