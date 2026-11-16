# FreshCart Pipelines

End-to-end Data Engineering assignment covering Git and CI/CD,
FreshCart ELT on Snowflake, Mage ML pipelines, and RabbitMQ streaming.

## Project Structure

- elt/ - FreshCart ELT pipeline
- iris_project/ - Mage Iris classification pipeline
- stream/ - RabbitMQ dispatcher and drivers
- tests/ - Automated tests
- .github/workflows/ - GitHub Actions CI

## FreshCart ELT (Part B)

The Snowflake pipeline is orchestrated by `elt.run_pipeline`. Configure the
Snowflake variables in `.env` using `.env.example` as a template. Run the
baseline with mock source data:

```powershell
python -m elt.run_pipeline --mode mock
```

Use `--mode transform-only` (the default) to rerun transformations against
already-loaded Bronze data without appending another extraction batch:

```powershell
python -m elt.run_pipeline --mode transform-only
```

Run an individual transformation stage with `--stage`, for example:

```powershell
python -m elt.run_pipeline --stage gold
```

Pincode responses use deterministic mock data unless `--live-pincodes` is
specified. SQL transformations are in `elt/sql/`; pure-Python rule tests are
run with `python -m pytest`. Live extraction requires the service URLs and
tokens documented as variable names in `.env.example`.

## Assignment Components

- Part A: Git and CI/CD
- Part B: FreshCart ELT on Snowflake
- Part C: ML Pipeline with Mage
- Part D: Food-order streaming with RabbitMQ
- Part E: Final report and viva
