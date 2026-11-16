import gzip
import json
import logging
import os
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import mock_data


log = logging.getLogger("extract")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def http_session():
    """Create an HTTP session with retry support."""

    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True
    )

    session = requests.Session()

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry)
    )

    session.mount(
        "http://",
        HTTPAdapter(max_retries=retry)
    )

    session.headers["User-Agent"] = "freshcart-elt/1.0"

    return session


def _auth(token_env):
    token = os.getenv(token_env)

    return (
        {"Authorization": f"Bearer {token}"}
        if token else {}
    )


def paginate(
    session,
    url,
    params=None,
    headers=None,
    items_key="data",
    next_key="next",
    timeout=30,
    max_pages=10000
):
    """Retrieve paginated API responses."""

    page_url = url
    page_params = dict(params or {})

    for _ in range(max_pages):

        response = session.get(
            page_url,
            params=page_params,
            headers=headers,
            timeout=timeout
        )

        response.raise_for_status()

        body = response.json()

        items = (
            body.get(items_key, [])
            if isinstance(body, dict)
            else body
        )

        yield from items

        next_page = (
            body.get(next_key)
            if isinstance(body, dict)
            else None
        )

        if not next_page:
            return

        page_url = next_page
        page_params = None

    raise RuntimeError(
        f"{url}: more than {max_pages} pages"
    )


def write_landing(source, records, run_id, run_date):
    """Write records as compressed newline-delimited JSON."""

    records = list(records)

    if not records:
        log.info("%s: 0 records, nothing written", source)
        return None

    landing_dir = Path(
        os.getenv("LANDING_DIR", "elt/landing")
    )
    if not landing_dir.is_absolute():
        landing_dir = PROJECT_ROOT / landing_dir

    folder = landing_dir / source / run_date.isoformat()

    folder.mkdir(parents=True, exist_ok=True)

    path = folder / f"{source}__{run_id}.json.gz"

    now = datetime.now(timezone.utc).isoformat()

    with gzip.open(path, "wt", encoding="utf-8") as file:

        for record in records:

            envelope = {
                "source": source,
                "run_id": run_id,
                "extracted_at": now,
                "data": record
            }

            file.write(
                json.dumps(
                    envelope,
                    ensure_ascii=False
                ) + "\n"
            )

    log.info(
        "%s: %d records -> %s",
        source,
        len(records),
        path
    )

    return path


def web_orders(session, run_date, mock=False):

    if mock:
        return mock_data.WEB_ORDERS

    since = (
        run_date - timedelta(days=1)
    ).isoformat()

    return paginate(
        session,
        os.environ["WEB_ORDERS_URL"],
        params={"updated_since": since},
        headers=_auth("WEB_ORDERS_TOKEN")
    )


def mobile_orders(session, run_date, mock=False):

    if mock:
        return mock_data.MOBILE_ORDERS

    since = (
        run_date - timedelta(days=1)
    ).isoformat()

    return paginate(
        session,
        os.environ["MOBILE_ORDERS_URL"],
        params={"updatedSince": since},
        headers=_auth("MOBILE_ORDERS_TOKEN"),
        items_key="orders",
        next_key="nextPage"
    )


def crm_tier_events(session, run_date, mock=False):

    if mock:
        return mock_data.CRM_TIER_EVENTS

    return paginate(
        session,
        os.environ["CRM_TIER_EVENTS_URL"],
        headers=_auth("CRM_TOKEN")
    )


def fx_rates(session, start, end, mock=False):
    """Retrieve daily USD-to-INR exchange rates."""

    if mock:
        return mock_data.FX_RESPONSES

    base_url = os.getenv(
        "FX_URL",
        "https://api.frankfurter.app"
    )

    output = []

    currencies = [
        currency.strip()
        for currency in os.getenv(
            "FX_BASES", "USD"
        ).split(",")
        if currency.strip()
    ]

    for base in currencies:

        response = session.get(
            f"{base_url}/{start.isoformat()}..{end.isoformat()}",
            params={
                "from": base,
                "to": "INR"
            },
            timeout=30
        )

        response.raise_for_status()

        output.append(response.json())

    return output


def pincodes(session, pins, mock=False):
    """Retrieve pincode details from India Post."""

    base_url = os.getenv(
        "PINCODE_URL",
        "https://api.postalpincode.in/pincode"
    )

    output = []

    for pin in pins:

        if mock:
            response = mock_data.pincode_response(pin)

        else:
            result = session.get(
                f"{base_url}/{pin}",
                timeout=30
            )

            result.raise_for_status()

            response = result.json()

            time.sleep(0.2)

        output.append({
            "pincode": pin,
            "response": response
        })

    return output