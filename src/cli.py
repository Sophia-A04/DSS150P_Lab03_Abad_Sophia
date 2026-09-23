import argparse

import pandas as pd

from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated
from src.validate.quality import validate_curated


def _latest_run_dir(base_dir):
    """Return the most recently modified run_id directory."""

    run_dirs = [
        path
        for path in base_dir.glob("run_id=*")
        if path.is_dir()
    ]

    if not run_dirs:
        raise FileNotFoundError(
            f"No pipeline run directories found under {base_dir}"
        )

    return max(
        run_dirs,
        key=lambda path: path.stat().st_mtime,
    )


def _load_staging(run_id: str):
    """Read staging Parquet outputs for one pipeline run."""

    staging_dir = (
        path_for("staging_dir")
        / f"run_id={run_id}"
    )

    return {
        name: pd.read_parquet(
            staging_dir / f"{name}.parquet"
        )
        for name in [
            "customers",
            "products",
            "orders",
        ]
    }


def _load_curated(run_id: str):
    """Read the curated sales dataset for one pipeline run."""

    curated_path = (
        path_for("curated_dir")
        / f"run_id={run_id}"
        / "sales_order_lines.parquet"
    )

    return pd.read_parquet(curated_path)


def _latest_run_id(base_dir):
    """Recover the run_id from the newest run-specific directory."""

    latest = _latest_run_dir(base_dir)

    return latest.name.removeprefix("run_id=")


def main():
    parser = argparse.ArgumentParser(
        description="DSS150P modular pipeline"
    )

    sub = parser.add_subparsers(
        dest="command",
        required=True,
    )

    sub.add_parser("validate-env")
    sub.add_parser("extract")
    sub.add_parser("transform")
    sub.add_parser("load")
    sub.add_parser("validate")

    benchmark_parser = sub.add_parser("benchmark")
    benchmark_parser.add_argument(
        "--repeats",
        type=int,
        default=5,
    )

    partition_parser = sub.add_parser("load-partition")
    partition_parser.add_argument(
        "--year",
        type=int,
        required=True,
    )
    partition_parser.add_argument(
        "--month",
        type=int,
        required=True,
    )

    sub.add_parser("run-all")

    args = parser.parse_args()

    # ---------------------------------------------------------
    # GOAL 1
    # ---------------------------------------------------------
    if args.command == "validate-env":
        print("PROJECT_ROOT=", PROJECT_ROOT)
        print(
            "DB host/database=",
            DB["host"],
            DB["dbname"],
        )
        print(
            "Configured source=",
            SETTINGS["pipeline"]["source_dir"],
        )
        return

    # ---------------------------------------------------------
    # GOAL 2 - EXTRACT
    # ---------------------------------------------------------
    if args.command == "extract":
        run_id = new_run_id()

        raw_dir = extract_sources(run_id)

        print("pipeline_run_id:", run_id)
        print("raw directory:", raw_dir)

        return

    # ---------------------------------------------------------
    # GOAL 2 - TRANSFORM
    # ---------------------------------------------------------
    if args.command == "transform":
        run_id = _latest_run_id(
            path_for("raw_dir")
        )

        raw_dir = (
            path_for("raw_dir")
            / f"run_id={run_id}"
        )

        staging, technical_quarantine = build_staging(
            raw_dir,
            run_id,
        )

        curated, orphan_quarantine = build_curated(
            staging,
            run_id,
        )

        print("pipeline_run_id:", run_id)
        print(
            "staging rows:",
            sum(len(frame) for frame in staging.values()),
        )
        print(
            "technical quarantine rows:",
            len(technical_quarantine),
        )
        print(
            "curated orphan rows:",
            len(orphan_quarantine),
        )
        print(
            "curated rows:",
            len(curated),
        )

        return

    # ---------------------------------------------------------
    # GOAL 2 - LOAD
    # ---------------------------------------------------------
    if args.command == "load":
        run_id = _latest_run_id(
            path_for("curated_dir")
        )

        curated = _load_curated(run_id)

        affected = upsert_curated(
            curated,
            run_id,
        )

        print("pipeline_run_id:", run_id)
        print("input rows:", len(curated))
        print("database rows affected:", affected)

        return

    # ---------------------------------------------------------
    # GOAL 2 - VALIDATE
    # ---------------------------------------------------------
    if args.command == "validate":
        run_id = _latest_run_id(
            path_for("curated_dir")
        )

        curated = _load_curated(run_id)

        errors = validate_curated(curated)

        print("pipeline_run_id:", run_id)
        print("curated rows:", len(curated))
        print("validation errors:", len(errors))

        if errors:
            for error in errors:
                print("-", error)

            raise RuntimeError(
                "Curated validation failed"
            )

        print("RESULT: PASS")

        return

    # ---------------------------------------------------------
    # GOAL 2 - COMPLETE PIPELINE
    # ---------------------------------------------------------
    if args.command == "run-all":
        run_id = new_run_id()

        print("pipeline_run_id:", run_id)

        raw_dir = extract_sources(run_id)
        print("extract: PASS")

        staging, technical_quarantine = build_staging(
            raw_dir,
            run_id,
        )
        print("staging: PASS")

        curated, orphan_quarantine = build_curated(
            staging,
            run_id,
        )
        print("curated: PASS")

        errors = validate_curated(curated)

        if errors:
            for error in errors:
                print("-", error)

            raise RuntimeError(
                "Curated validation failed"
            )

        print("validation: PASS")

        affected = upsert_curated(
            curated,
            run_id,
        )

        print("load: PASS")
        print("database rows affected:", affected)
        print(
            "technical quarantine rows:",
            len(technical_quarantine),
        )
        print(
            "curated orphan rows:",
            len(orphan_quarantine),
        )
        print("curated rows:", len(curated))

        print("\nPIPELINE RESULT: PASS")

        return

    # Goal 3 commands remain intentionally unwired.
    if args.command in {
        "benchmark",
        "load-partition",
    }:
        raise NotImplementedError(
            f"{args.command} belongs to Goal 3"
        )


if __name__ == "__main__":
    main()