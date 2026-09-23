# DSS150P Laboratory Activity #3
## Productionizing a Modular Data Pipeline

This repository contains the completed implementation for DSS150P Laboratory Activity #3.

The laboratory develops a modular and rerun-safe data pipeline covering reproducible environments, ETL/ELT transformations, storage benchmarking, partitioned loading, PostgreSQL persistence, and workflow orchestration using Apache Airflow.

## Main Progression

- Goal 1: Reproducible environment, modularization, Git, Docker, and configuration
- Goal 2: Raw -> staging -> curated transformations, audit/error handling, quarantine, and rerun-safe loading
- Goal 3: CSV/JSONL/Parquet/PostgreSQL comparison, benchmarking, partitioning, and selected-partition loading
- Goal 4: Apache Airflow DAG for `extract -> transform -> load -> validate`
- `docs/technical_questions.md`
- `docs/benchmark_results.csv`
- `docs/benchmark_context.json`
- `evidence/README.md`

## Recommended Commands

Create the local environment:

```bash
cp .env.example .env
python -m venv .venv
```

Activate the virtual environment, then install the required packages:

```bash
pip install -r requirements.txt
python -m src.cli validate-env
```

The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

### Docker / PostgreSQL

```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

### Apache Airflow

Initialize Airflow:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
```

Start the Airflow webserver and scheduler:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Airflow UI:

`http://localhost:8080`

Local laboratory credentials: `admin/admin`

## Running the Pipeline

### Environment Validation

```bash
python -m src.cli validate-env
```

### Full Pipeline

```bash
python -m src.cli run-all
```

### Curated Data Validation

```bash
python -m src.cli validate
```

### Storage Benchmark

Run the controlled storage benchmark using five repetitions:

```bash
python -m src.cli benchmark --repeats 5
```

### Selected Partition Load

Load the demonstrated January 2025 partition:

```bash
python -m src.cli load-partition --year 2025 --month 1
```

The demonstrated partition contains 2,458 rows.

## Airflow Validation

Check that the DAG imports without parse errors:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags list-import-errors
```

List the tasks in the Airflow DAG:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow tasks list dss150p_sales_pipeline
```

The DAG contains the dependency sequence:

`extract -> transform -> load -> validate`

The DAG supports the following parameters:

- `run_mode=full`
- `run_mode=partition`
- `year`
- `month`

The demonstrated parameterized partition run used:

- `run_mode=partition`
- `year=2025`
- `month=1`
- Partition rows: 2,458

## Verified Pipeline Results

The final integrated acceptance run confirmed:

- Curated rows: 49,897
- Validation errors: 0
- PostgreSQL total rows: 49,897
- Distinct `order_id` values: 49,897
- Selected January 2025 partition: 2,458 rows
- Airflow DAG import errors: none
- Full pipeline result: PASS
- Storage benchmark result: PASS
- Partition load result: PASS

Repeated loading did not create duplicate business rows.

## Evidence and Documentation

Detailed laboratory evidence is available in:

- `docs/goal1_environment.md`
- `docs/goal1_modularization.md`
- `docs/goal2_etl.md`
- `docs/goal3_storage.md`
- `docs/goal4_airflow.md`
- `docs/integrated_acceptance.md`
- `docs/technical_reflection.md`

These documents record environment validation, ETL/ELT behavior, quarantine handling, storage benchmarking, partition loading, Airflow orchestration, controlled failure and recovery, and the final integrated acceptance checks.

## Tests

Run the transformation rule tests with:

```bash
python -m unittest discover -s tests -v

## AI Usage

I used ChatGPT and Gemini as an AI tool during this laboratory activity. I needed help to understand some of the laboratory instructions, I asked questions about some steps and asked them to explain technical terms in simpler words, troubleshoot errors on my laptop that appeared while running commands, and organize some parts of the Python code and documentation.

I ran the commands and scripts myself and used the actual outputs from my environment when recording profiling results. I read the generated code and documentation before adding them to the repository. I would ask what this and that would do, because I was unfamiliar with GitHub. I think I am familiarized with how to navigate GitHub and VS code, words like "git status" and "git add" appear a lot while I am working on the terminal.
