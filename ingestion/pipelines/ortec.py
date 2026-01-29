import dlt
from dlt.sources.sql_database import sql_database

if __name__ == "__main__":

    source = sql_database(schema="bi_support")

    pipeline = dlt.pipeline(
        pipeline_name="ortec",
        destination="odiondataplatform",
        dataset_name="raw_ortec",
    )

    tables = [
        "DIM_COST_CENTER",
        "DIM_DATE",
        "DIM_EMPLOYEE",
        "DIM_LOCATION",
        "DIM_TIME",
        "FACT_PUBLISHED_SHIFT",
    ]

    # TODO: incremental loads for large tables
    info = pipeline.run(
        source.with_resources(*tables),
        write_disposition="replace",
    )

    print(info)
