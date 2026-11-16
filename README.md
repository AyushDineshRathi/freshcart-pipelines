# FreshCart Data Engineering Pipelines

FreshCart is an end-to-end data engineering assignment that brings together
source control and CI, a Snowflake ELT pipeline, a Mage machine-learning
workflow, and a RabbitMQ event-streaming demo.

## Contents

- [Project overview](#project-overview)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Install and configure](#install-and-configure)
- [Workflow 1: FreshCart ELT on Snowflake](#workflow-1-freshcart-elt-on-snowflake)
- [Workflow 2: Iris classification with Mage](#workflow-2-iris-classification-with-mage)
- [Workflow 3: RabbitMQ food-order dispatch](#workflow-3-rabbitmq-food-order-dispatch)
- [Workflow 4: Git and continuous integration](#workflow-4-git-and-continuous-integration)
- [Tests and quality checks](#tests-and-quality-checks)
- [Troubleshooting](#troubleshooting)

## Project overview

| Area | Purpose | Main technology |
|---|---|---|
| Part A | Version control and automated test checks | Git, GitHub Actions |
| Part B | Extract, ingest, validate, transform, and publish FreshCart data | Python, Snowflake |
| Part C | Train and evaluate an Iris classifier | Mage, scikit-learn |
| Part D | Publish food/ride dispatch events and route them to consumers | RabbitMQ |
| Part E | Assignment report and presentation material | Word documents |

The FreshCart ELT flow brings web orders, mobile orders, customer tier events,
foreign-exchange rates, and postal-code lookups through raw Bronze storage,
validated and deduplicated Silver models, reference/dimension data, and a Gold
orders fact table. The other workflows can be run independently.

## Repository layout

```text
.
├── .github/workflows/ci.yml       # GitHub Actions test workflow
├── elt/
│   ├── extract.py                 # API/mock extraction and landing files
│   ├── landing/                   # Compressed NDJSON source files
│   ├── load_bronze.py             # Standalone landing-to-Bronze loader
│   ├── pincode_lookup.py          # Incremental geography lookup
│   ├── quality_checks.py          # Snowflake quality checks and metrics
│   ├── run_pipeline.py            # ELT command-line orchestration
│   ├── snowflake_io.py            # Snowflake connection and file loading
│   ├── transformations.py         # Python equivalents of transformation rules
│   └── sql/                       # Snowflake setup, migrations, and models
├── iris_project/                  # Mage project, blocks, and Iris artifacts
├── mage_data/                     # Mage runtime data
├── stream/
│   ├── dispatcher.py              # RabbitMQ topic publisher
│   └── driver.py                  # RabbitMQ topic consumer
├── tests/                         # Pytest suite
├── Dockerfile.mage                # Mage image definition
├── requirements.txt               # Python dependencies
└── README.md
```

## Prerequisites

- Python 3.12 is the version used by CI. Use a compatible Python 3.10+ runtime
  for local development.
- Git.
- Docker Desktop or another Docker-compatible runtime for Mage and RabbitMQ.
- A Snowflake account, user, role, and warehouse to run the ELT pipeline.
- Network access and source API credentials only when running live extraction.

## Install and configure

From the repository root, create and activate a virtual environment and install
the project dependencies.

### Windows PowerShell

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### macOS or Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For Snowflake ELT, create a local `.env` from the supplied template:

```powershell
Copy-Item .env.example .env
```

Set the Snowflake account, username, password, role, and warehouse values in
`.env`. The configured role must be allowed to use the warehouse and create or
use the database objects in `FRESHCART`. `LANDING_DIR` defaults to
`elt/landing` and can be changed to another relative or absolute path.

For live source extraction, also set the required `WEB_ORDERS_URL`,
`MOBILE_ORDERS_URL`, and `CRM_TIER_EVENTS_URL` values. Their tokens are
optional if the corresponding API does not require bearer authorization:
`WEB_ORDERS_TOKEN`, `MOBILE_ORDERS_TOKEN`, and `CRM_TOKEN`. The FX endpoint
defaults to Frankfurter; `FX_URL` and `FX_BASES` can override its endpoint and
base currency/currencies. Do not commit `.env` or paste credentials into
source files.

Mock ELT data does not require live source API URLs or tokens, but it still
requires Snowflake credentials because it loads and transforms data in
Snowflake.

## Workflow 1: FreshCart ELT on Snowflake

### Data flow

```text
Web orders ─┐
Mobile orders ─┤
CRM tier events ─┼─> Extract to compressed NDJSON landing files
FX rates ────────┘                    │
                                      v
                         Snowflake internal stage
                                      │
                                      v
                                  Bronze raw
                                      │
                         Validate and normalize
                                      │
                         Silver clean / quarantine
                                      │
                         Silver deduplicated orders
                                      │
                  Tier SCD2 + FX + postal geography
                                      │
                                      v
                              Gold order fact
                                      │
                         Blocking checks + metrics
```

Extracted files are written under
`elt/landing/<source>/<YYYY-MM-DD>/<source>__<run-id>.json.gz`. Records are
stored as newline-delimited JSON envelopes containing source, run ID,
extraction time, and the original record. The available sources are `web`,
`mobile`, `crm`, and `fx`; missing postal-code reference data is looked up
incrementally from orders.

### Run the complete pipeline with mock source data

```powershell
python -m elt.run_pipeline --mode mock
```

This command runs Snowflake setup and the ingestion-ledger migration, extracts
deterministic mock web/mobile/CRM/FX data, loads the files into Bronze, then
runs all stages and blocking quality checks. Pincode lookup responses are
mocked by default.

### Run with live source APIs

After configuring the source URLs and any required tokens in `.env`, run:

```powershell
python -m elt.run_pipeline --mode live
```

Live extraction is performed only when `--stage all` is selected. API
pagination uses HTTPS and retries transient request failures. Live postal-code
lookups are a separate option; add `--live-pincodes` to call the India Post
endpoint for missing pincodes instead of using mock responses:

```powershell
python -m elt.run_pipeline --mode live --live-pincodes
```

### Re-run transformations or a single stage

The default mode is `transform-only`. It does not extract or reload Bronze
files, so it is suitable for rerunning transformations against data already
loaded into Snowflake:

```powershell
python -m elt.run_pipeline
python -m elt.run_pipeline --mode transform-only
```

Run one transformation or quality stage with `--stage`:

```powershell
python -m elt.run_pipeline --stage clean
python -m elt.run_pipeline --stage dedup
python -m elt.run_pipeline --stage pincode
python -m elt.run_pipeline --stage scd2
python -m elt.run_pipeline --stage fx
python -m elt.run_pipeline --stage geo
python -m elt.run_pipeline --stage gold
python -m elt.run_pipeline --stage quality
```

Valid stage names are `all`, `clean`, `dedup`, `pincode`, `scd2`, `fx`, `geo`,
`gold`, and `quality`. A single stage never performs source extraction. For
example, `--mode mock --stage gold` runs only the Gold transformation after
setup; it does not create a new mock extraction batch.

The pipeline generates a unique run ID automatically. To supply one for
traceability, use:

```powershell
python -m elt.run_pipeline --mode mock --run-id freshcart_demo_001
```

### ELT models and behavior

The SQL stages are maintained in `elt/sql/`:

| Stage | SQL file | Result |
|---|---|---|
| Setup | `00_setup.sql` | Creates the warehouse/database/schemas, raw tables, reference tables, quarantine table, Gold table, and run log if needed |
| Ingestion ledger | `01_ingestion_files.sql` | Creates the file-ingestion tracking table |
| Clean | `10_orders_clean.sql` | Normalizes and validates orders; routes invalid records to quarantine |
| Dedup | `20_orders_dedup.sql` | Keeps the latest record per order, preferring mobile on exact timestamp ties |
| Customer tiers | `30_customer_tier_scd2.sql` | Applies customer tier history as a Type 2 dimension |
| FX | `31_fx_rates.sql` | Prepares exchange rates used by Gold |
| Geography | `32_pincode_geo.sql` | Resolves postal-code geography and regional data |
| Gold | `40_gold.sql` | Publishes the enriched order fact table |

`--stage all` runs the stages in the order `clean`, `dedup`, `pincode`, `scd2`,
`fx`, `geo`, `gold`, and `quality`. The quality stage checks that valid and
invalid orders are accounted for, deduplicated and Gold order IDs are unique,
and Gold orders have customer, FX, and geography references. A nonzero
blocking-check result fails the run. The report also prints Bronze, clean,
deduplicated, Gold, quarantine, and rejection metrics, Gold revenue, and
loyalty points. Each setup, ingestion, and transformation step is recorded in
`FRESHCART.OPS.RUN_LOG`.

### Safe file ingestion and recovery

Bronze ingestion uses `FRESHCART.OPS.INGESTION_FILES` to track each staged
file. Its logical identity is derived from source and staged path; the
compressed file's SHA-256 checksum detects changed content at an existing
path. Files already marked `LOADED` are not loaded again. Failed operations
are marked `FAILED` and can be retried after the underlying issue is resolved.

The ingestion ledger is a standard Snowflake table; Snowflake does not enforce
its `FILE_ID` uniqueness. A local host lock prevents competing loader
processes on that host, but it does not coordinate writers running on other
hosts. Keep all scheduled jobs and manual loaders single-writer across the
environment.

Snowflake `PUT`, `COPY`, and ledger updates are separate operations, not one
atomic transaction. If a process stops during ingestion and leaves a `LOADING`
record, first make sure no loader is active, then inspect the corresponding
Bronze `SRC_FILE` row count and reconcile the ledger before retrying. Do not
manually change an entry based only on its status.

### Standalone Bronze loading

If landing files already exist and you only want to load them, run:

```powershell
python -m elt.load_bronze
```

This loads available `.json.gz` files from the configured landing directory
into the mapped Bronze tables. It does not extract data or run the Silver/Gold
transformations.

## Workflow 2: Iris classification with Mage

The Mage project is `iris_project/`; the `iris_classifier` pipeline loads all
150 scikit-learn Iris samples, prepares a deterministic 80:20 train/test split
(`random_state=42`), and trains a Decision Tree (`random_state=42`). Its block
flow is:

```text
load_iris -> prepare_data -> train_model -> save_results
                                      \-> evaluate_model
```

Training tests validate the held-out accuracy and classification report before
downstream blocks can run. A failed training test prevents model/result
export. `max_depth` is a runtime variable with a default of `3` and an allowed
range of 1–5.

### Build and start Mage

From the repository root:

```powershell
docker build -f Dockerfile.mage -t freshcart-mage .
docker run -d --name freshcart-mage -p 6789:6789 -v "${PWD}:/home/src" freshcart-mage mage start /home/src/iris_project
```

Mage is then available at <http://localhost:6789>. The repository root is
mounted into the container at `/home/src`, which lets Mage use the checked-out
project and makes generated artifacts visible on the host. To follow startup
logs:

```powershell
docker logs -f freshcart-mage
```

If the container already exists, start it with `docker start freshcart-mage`.
To stop it, use `docker stop freshcart-mage`. The Docker image is built from
`Dockerfile.mage`, which starts with Mage's image and pins compatible SQLAlchemy
and urllib3 versions.

### Run the Mage pipeline and vary the tree depth

Run a tested pipeline execution from the host:

```powershell
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 3}' /home/src/iris_project iris_classifier
```

Use any integer depth from 1 to 5 to compare model results:

```powershell
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 1}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 2}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 3}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 4}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 5}' /home/src/iris_project iris_classifier
```

`--test` enables Mage block tests. The project-specific architecture,
runtime-variable details, and artifact descriptions are also documented in
[the Iris project README](iris_project/README.md).

### Mage output

Successful runs write artifacts under `iris_project/`:

- `models/iris_decision_tree.joblib` — trained model.
- `results/iris_evaluation.json` — accuracy, classification report, split
  sizes, depth, and model/prediction paths.
- `results/iris_predictions.csv` — predictions for the 30 held-out records.
- `results/iris_confusion_matrix.csv` — labeled 3×3 confusion matrix.
- `results/iris_metrics.csv` — per-class, macro, weighted, and accuracy
  metrics.
- `results/iris_model_evaluation.json` — structured evaluation and artifact
  paths.

Each training attempt clears a previous predictions file before running, so a
failed attempt cannot leave stale predictions appearing to be current.

## Workflow 3: RabbitMQ food-order dispatch

This workflow consists of a topic-exchange publisher and one or more
consumers. It expects a RabbitMQ broker reachable at `localhost:5672` unless
`RABBITMQ_URL` is set in the environment or local `.env`.

Start a local broker with Docker (the management UI is optional):

```powershell
docker run -d --name freshcart-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

The management UI, when enabled, is at <http://localhost:15672>. If a broker
already runs elsewhere, set `RABBITMQ_URL` to its AMQP connection URL instead.
Start the publisher in one terminal:

```powershell
python -m stream.dispatcher
```

It declares the durable `rapido-dispatch` topic exchange and publishes a
message every two seconds with routing keys in the form
`<city>.<zone>.<type>`. The demo covers Bangalore and Chennai and ride and food
request types. Every tenth message intentionally omits its `zone` field.

Start a consumer in another terminal. Its arguments are a display name and a
RabbitMQ topic binding pattern:

```powershell
python -m stream.driver "Bangalore food" "bangalore.*.food"
```

Other useful bindings include `bangalore.#` (all Bangalore messages),
`*.koramangala.*` (that zone in any city/type), and `#` (all messages). Each
driver declares a private, exclusive, auto-delete queue. Valid JSON messages
with a `zone` are acknowledged; malformed JSON or messages without a zone are
rejected with a non-requeueing NACK.

Stop either process with `Ctrl+C`. Stop the local RabbitMQ container when
finished:

```powershell
docker stop freshcart-rabbitmq
```

## Workflow 4: Git and continuous integration

The GitHub Actions workflow in `.github/workflows/ci.yml` runs for pushes and
pull requests targeting `main`. It checks out the repository, sets up Python
3.12, installs `requirements.txt`, and runs:

```bash
python -m pytest -v
```

A typical contribution flow is:

1. Create a feature branch from `main`.
2. Make a focused change and add or update relevant tests.
3. Run the tests locally with `python -m pytest -v`.
4. Push the branch and open a pull request targeting `main`.
5. Confirm the **FreshCart CI** workflow passes before merging.

## Tests and quality checks

Run the full Python test suite from the repository root:

```powershell
python -m pytest -v
```

The suite includes ELT transformation and extraction tests, Snowflake loader
and orchestration reliability tests, and Iris block/pipeline tests. The Mage
gate integration test requires Docker and a running container named
`freshcart-mage` with the repository mounted at `/home/src`; without that
environment, pytest skips the integration test. GitHub Actions runs the
pytest command but does not start Snowflake, Mage, or RabbitMQ services.

The ELT quality stage is separate from pytest because it queries the configured
Snowflake account:

```powershell
python -m elt.run_pipeline --stage quality
```

It exits with an error if any blocking Snowflake check fails.

## Troubleshooting

### Snowflake reports missing environment variables

Check that `.env` exists at the repository root and contains
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`,
and `SNOWFLAKE_WAREHOUSE`. Ensure the values are actual credentials, not the
placeholders from `.env.example`.

### Live extraction fails before landing files are written

For live mode, verify the web, mobile, and CRM endpoint URLs, token names, API
availability, and response contract. Live API extraction requires HTTPS and
valid paginated JSON responses. Use `--mode mock` to exercise the Snowflake
pipeline without calling those source APIs.

### Mage commands cannot find the container or project

Confirm Docker is running and that the container is named `freshcart-mage`.
Check that it is running with `docker ps` and that the repository is mounted at
`/home/src`. Rebuild the image after changing `Dockerfile.mage`.

### RabbitMQ connection is refused

Confirm the broker is running and listening on port 5672. If it is not local,
set `RABBITMQ_URL` to the correct broker URL before launching both publisher
and consumer. Keep the connection URL and credentials out of committed files.
