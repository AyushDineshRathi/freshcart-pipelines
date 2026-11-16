
import os
import re
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


BRONZE_TABLES = {
    "web": "FRESHCART.BRONZE.ORDERS_RAW",
    "mobile": "FRESHCART.BRONZE.ORDERS_RAW",
    "crm": "FRESHCART.BRONZE.CUSTOMERS_RAW",
    "fx": "FRESHCART.BRONZE.FX_RAW",
    "pincode": "FRESHCART.BRONZE.PINCODE_RAW",
}

BRONZE_STAGE = "@FRESHCART.BRONZE.LANDING"
FILE_FORMAT = "FRESHCART.BRONZE.FF_NDJSON"


def get_snowflake_connection():
    """Create a Snowflake connection using local environment variables."""

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: "
            f"{', '.join(missing)}"
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="FRESHCART",
        schema="BRONZE",
    )


def test_connection():
    """Verify connectivity and print non-sensitive session details."""

    connection = get_snowflake_connection()

    try:
        cursor = connection.cursor()

        try:
            cursor.execute("""
                SELECT
                    CURRENT_ACCOUNT_NAME(),
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE()
            """)

            result = cursor.fetchone()

            print("Snowflake connection successful!")
            print("Account:", result[0])
            print("User:", result[1])
            print("Role:", result[2])
            print("Warehouse:", result[3])
            print("Database:", result[4])

        finally:
            cursor.close()

    finally:
        connection.close()


def load_file(connection, local_path, source):
    """
    Upload one compressed NDJSON file and load its raw envelopes
    into the appropriate Bronze table.

    Returns the COPY INTO result.
    """

    if source not in BRONZE_TABLES:
        raise ValueError(f"Unsupported source: {source}")

    local_path = Path(local_path).resolve()

    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    if local_path.suffixes[-2:] != [".json", ".gz"]:
        raise ValueError(
            "Expected a compressed .json.gz NDJSON file"
        )

    if not re.fullmatch(r"[A-Za-z0-9_.-]+", local_path.name):
        raise ValueError("Invalid source filename")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", source):
        raise ValueError("Invalid source name")

    run_date = local_path.parent.name

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise ValueError(
            f"Expected a YYYY-MM-DD parent directory, got {run_date}"
        )

    target_table = BRONZE_TABLES[source]

    # Stage path is unique by source and extraction date.
    stage_prefix = f"{source}/{run_date}"
    stage_file = f"{stage_prefix}/{local_path.name}"

    # Windows paths must be converted to forward slashes for PUT.
    file_uri = local_path.as_posix()

    if not file_uri.startswith("/"):
        file_uri = "/" + file_uri

    put_sql = (
        f"PUT 'file://{file_uri}' "
        f"{BRONZE_STAGE}/{stage_prefix}/ "
        "AUTO_COMPRESS=FALSE "
        "OVERWRITE=TRUE"
    )

    cursor = connection.cursor()

    try:
        print(f"\nUploading: {local_path.name}")

        cursor.execute(put_sql)

        put_results = cursor.fetchall()

        for result in put_results:
            print("PUT:", result)

        # Preserve the envelope metadata and raw data separately.
        copy_sql = f"""
            COPY INTO {target_table} (
                SOURCE,
                RAW,
                RUN_ID,
                EXTRACTED_AT,
                SRC_FILE,
                SRC_ROW
            )
            FROM (
                SELECT
                    $1:source::STRING,
                    $1:data::VARIANT,
                    $1:run_id::STRING,
                    TRY_TO_TIMESTAMP_TZ(
                        $1:extracted_at::STRING
                    ),
                    METADATA$FILENAME,
                    METADATA$FILE_ROW_NUMBER
                FROM {BRONZE_STAGE}
            )
            FILES = ('{stage_file}')
            FILE_FORMAT = (
                FORMAT_NAME = '{FILE_FORMAT}'
            )
            ON_ERROR = 'ABORT_STATEMENT'
        """

        cursor.execute(copy_sql)

        copy_results = cursor.fetchall()

        print(f"COPY INTO {target_table}:")

        for result in copy_results:
            print(result)

        return copy_results

    finally:
        cursor.close()


if __name__ == "__main__":
    test_connection()
