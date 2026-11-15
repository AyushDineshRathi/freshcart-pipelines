from datetime import date
from pathlib import Path
import gzip
import json

from elt import extract


run_id = "baseline_test_001"
run_date = date(2026, 9, 30)

session = extract.http_session()

sources = {
    "web": extract.web_orders(session, run_date, mock=True),
    "mobile": extract.mobile_orders(session, run_date, mock=True),
    "crm": extract.crm_tier_events(session, run_date, mock=True),
    "fx": extract.fx_rates(
        session,
        date(2026, 8, 26),
        run_date,
        mock=True
    )
}

for source, records in sources.items():

    path = extract.write_landing(
        source,
        records,
        run_id,
        run_date
    )

    print(
        f"{source}: {len(records)} records | {path}"
    )

    assert path is not None
    assert Path(path).exists()

    with gzip.open(path, "rt", encoding="utf-8") as file:
        first_record = json.loads(file.readline())

    assert "source" in first_record
    assert "run_id" in first_record
    assert "extracted_at" in first_record
    assert "data" in first_record

print("\nExtraction test completed successfully.")