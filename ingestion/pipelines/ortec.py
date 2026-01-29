from datetime import datetime

import dlt
from dlt.sources.sql_database import sql_database


def ingest_ortec() -> None:

    source = sql_database(schema="bi_support")

    pipeline = dlt.pipeline(
        pipeline_name="ortec",
        destination="odiondataplatform",
        dataset_name="raw_ortec",
    )

    dims = [
        "DIM_COST_CENTER",
        "DIM_DATE",
        "DIM_EMPLOYEE",
        "DIM_LOCATION",
        "DIM_TIME",
    ]

    # full refreshes for dimensions
    info = pipeline.run(
        source.with_resources(*dims),
        write_disposition="replace",
    )

    # Fact: incremental
    fact = source.with_resources("FACT_PUBLISHED_SHIFT")
    fact.resources["FACT_PUBLISHED_SHIFT"].apply_hints(
        primary_key="SHIFT_ID",
        incremental={
            "cursor_path": "DATE_CREATED",
            "initial_value": datetime(2025, 1, 1),
        },
    )

    info = pipeline.run(fact, write_disposition="merge")

    print(info)


if __name__ == "__main__":
    ingest_ortec()
