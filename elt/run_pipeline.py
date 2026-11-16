"""Command-line orchestration for the FreshCart Snowflake ELT pipeline."""

import argparse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys
from uuid import uuid4

from dotenv import load_dotenv

from . import extract
from .pincode_lookup import load_missing_pincodes
from .quality_checks import quality_checks
from .snowflake_io import get_snowflake_connection, load_file


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = Path(__file__).resolve().parent / "sql"

SQL_STAGES = {
    "clean": "10_orders_clean.sql",
    "dedup": "20_orders_dedup.sql",
    "scd2": "30_customer_tier_scd2.sql",
    "fx": "31_fx_rates.sql",
    "geo": "32_pincode_geo.sql",
    "gold": "40_gold.sql",
}
ALL_STAGES = (
    "clean",
    "dedup",
    "pincode",
    "scd2",
    "fx",
    "geo",
    "gold",
    "quality",
)


def _run_sql_file(connection, file_name: str) -> None:
    path = SQL_DIR / file_name
    if not path.is_file():
        raise FileNotFoundError(path)

    cursors = list(connection.execute_string(
        path.read_text(encoding="utf-8"),
        remove_comments=True,
    ))
    for cursor in cursors:
        cursor.close()


def _log_step(
    connection,
    run_id: str,
    step: str,
    status: str,
    detail: str,
) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO FRESHCART.OPS.RUN_LOG
              (RUN_ID, STEP, STATUS, DETAIL)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, step, status, detail[:4000]),
        )
    finally:
        cursor.close()


def _run_logged_step(connection, run_id: str, step: str, action):
    try:
        result = action()
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"
        _log_step(connection, run_id, step, "FAILED", detail)
        raise
    _log_step(connection, run_id, step, "SUCCEEDED", "Completed")
    return result


def _extract_sources(
    run_id: str, mock: bool, run_date: date
) -> list[tuple[str, Path]]:
    session = extract.http_session()
    try:
        fx_start = run_date - timedelta(days=35)
        source_iterables = {
            "web": extract.web_orders(session, run_date, mock=mock),
            "mobile": extract.mobile_orders(session, run_date, mock=mock),
            "crm": extract.crm_tier_events(session, run_date, mock=mock),
            "fx": extract.fx_rates(
                session,
                fx_start,
                run_date,
                mock=mock,
            ),
        }
        source_records = {
            source: list(records)
            for source, records in source_iterables.items()
        }
    finally:
        session.close()

    written = []
    for source, records in source_records.items():
        path = extract.write_landing(source, records, run_id, run_date)
        if path is not None:
            written.append((source, path))
    return written


def _load_extracted_files(connection, files: list[tuple[str, Path]]) -> None:
    for source, path in files:
        load_file(connection, path, source)


def run_pipeline(
    mode: str = "transform-only",
    stage: str = "all",
    run_id: str | None = None,
    live_pincodes: bool = False,
) -> dict[str, object]:
    """Run the selected pipeline stages and always close the connection."""
    load_dotenv(PROJECT_ROOT / ".env")
    run_id = run_id or (
        "freshcart_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "_" + uuid4().hex[:8]
    )
    run_date = date.today()
    selected_stages = ALL_STAGES if stage == "all" else (stage,)
    connection = get_snowflake_connection()

    try:
        _run_logged_step(
            connection,
            run_id,
            "setup",
            lambda: _run_sql_file(connection, "00_setup.sql"),
        )

        if mode in {"mock", "live"} and stage == "all":
            files = _run_logged_step(
                connection,
                run_id,
                "extract",
                lambda: _extract_sources(
                    run_id,
                    mock=(mode == "mock"),
                    run_date=run_date,
                ),
            )
            _run_logged_step(
                connection,
                run_id,
                "load_bronze",
                lambda: _load_extracted_files(connection, files),
            )
        elif mode != "transform-only":
            print(
                "A new extraction is performed only with "
                "--mode mock/live --stage all."
            )
        else:
            print(
                "Transformation-only mode: no extraction or Bronze load "
                "will be performed."
            )

        results: dict[str, object] = {"run_id": run_id}
        for current_stage in selected_stages:
            if current_stage in SQL_STAGES:
                _run_logged_step(
                    connection,
                    run_id,
                    current_stage,
                    lambda file_name=SQL_STAGES[current_stage]:
                        _run_sql_file(connection, file_name),
                )
            elif current_stage == "pincode":
                results["pincodes_loaded"] = _run_logged_step(
                    connection,
                    run_id,
                    "pincode_lookup",
                    lambda: load_missing_pincodes(
                        connection,
                        run_id,
                        mock=not live_pincodes,
                        run_date=run_date,
                    ),
                )
            elif current_stage == "quality":
                results["quality"] = _run_logged_step(
                    connection,
                    run_id,
                    "quality_checks",
                    lambda: quality_checks(connection),
                )
        return results
    finally:
        connection.close()


def _parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("mock", "live", "transform-only"),
        default="transform-only",
        help=(
            "mock/live performs a fresh extraction and Bronze load only "
            "with --stage all; transform-only never extracts."
        ),
    )
    parser.add_argument(
        "--stage",
        choices=("all", *ALL_STAGES),
        default="all",
        help="Run the full pipeline or one stage; individual stages do not extract.",
    )
    parser.add_argument(
        "--run-id",
        help="Optional run identifier; a unique identifier is generated by default.",
    )
    parser.add_argument(
        "--live-pincodes",
        action="store_true",
        help="Call India Post for missing pincodes instead of using mock responses.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = _parse_args(argv)
    try:
        run_pipeline(
            mode=arguments.mode,
            stage=arguments.stage,
            run_id=arguments.run_id,
            live_pincodes=arguments.live_pincodes,
        )
    except Exception as error:
        print(f"Pipeline failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
