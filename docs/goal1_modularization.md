# Goal 1 - Task 7.2: Modular Pipeline Structure

## Purpose

The pipeline is organized into separate modules so that each part of the system has one primary responsibility. This keeps extraction, transformation, loading, validation, and benchmarking logic separate from command-line orchestration.

## Module Responsibilities

| Module | Primary Responsibility | Must NOT Contain |
|---|---|---|
| `src/extract/` | Acquire or copy source snapshots into the raw data layer | Business calculations |
| `src/transform/` | Perform staging cleanup and curated business rules | Airflow-specific code |
| `src/load/` | Handle PostgreSQL persistence and UPSERT operations | Source-specific cleaning |
| `src/validate/` | Perform data quality and contract assertions | Transformation side effects |
| `src/benchmark/` | Materialize storage formats and measure performance | Production business logic |
| `src/cli.py` | Provide thin command-line entry points and coordinate the modules | Duplicate transformation implementations |

## CLI Coordination

The `src/cli.py` file acts as the command-line entry point for the pipeline. Its responsibility is to accept commands and coordinate the appropriate reusable modules.

The CLI should remain thin. Business rules, data cleaning, database loading, validation, and benchmarking logic should be implemented inside their corresponding modules instead of directly inside `cli.py`.

The starter CLI currently provides commands for:

- `validate-env`
- `extract`
- `transform`
- `load`
- `validate`
- `benchmark`
- `load-partition`
- `run-all`

At this stage, `validate-env` is already functional. The remaining commands will be connected to their respective module implementations during the later goals of the laboratory activity.

## Current Structure Assessment

The provided starter repository already follows the required modular structure. Therefore, no major restructuring is necessary for Task 7.2.

The existing separation allows the same reusable Python modules to be called from both the CLI and, later, the Airflow DAG without placing transformation or business logic directly inside the DAG.

This structure improves:

- separation of concerns;
- readability and maintainability;
- independent testing of pipeline components;
- reuse of the same pipeline logic by the CLI and Airflow; and
- prevention of duplicated transformation logic.

## Implementation Decision

Functions that belong to later laboratory goals will not be implemented prematurely. Each function will be completed under the task that owns its behavior while preserving the modular responsibilities defined above.