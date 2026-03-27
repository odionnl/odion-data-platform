import dlt
import pandas as pd
from sqlalchemy import URL, create_engine


ORTEC_QUERY = """
SELECT
    CAST(CONCAT(FORMAT(dd.FULL_DATE, 'yyyy-MM-dd'), ' ',
         FORMAT(dt1.TIME24, 'HH:mm:ss')) AS DATETIME) AS start_time,
    CAST(CONCAT(FORMAT(dd.FULL_DATE, 'yyyy-MM-dd'), ' ',
         FORMAT(dt2.TIME24, 'HH:mm:ss')) AS DATETIME) AS end_time,
    de.EMPLOYEE_NUMBER AS employee_id,
    de.NAME AS employee_name,
    dcc.NAME AS cost_center_id,
    dcc.DESCRIPTION AS cost_center_name
FROM bi_support.FACT_PUBLISHED_SHIFT AS fps
    LEFT JOIN bi_support.DIM_COST_CENTER AS dcc ON dcc.COST_CENTER_KEY=fps.COST_CENTER_KEY
    LEFT JOIN bi_support.DIM_DATE AS dd ON dd.DATE_KEY=fps.BEGIN_DATE_KEY
    LEFT JOIN bi_support.DIM_TIME AS dt1 ON dt1.TIME_KEY=fps.START_TIME_KEY
    LEFT JOIN bi_support.DIM_TIME AS dt2 ON dt2.TIME_KEY=fps.END_TIME_KEY
    LEFT JOIN bi_support.DIM_EMPLOYEE AS de ON de.EMPLOYEE_KEY=fps.EMPLOYEE_KEY
WHERE FULL_DATE >= DATEADD(month, -3, CAST(GETDATE() AS DATE))
    AND ROSTER_STATUS IN ('Processed', 'Published')
"""


@dlt.resource(name="diensten", write_disposition="replace")
def ortec_diensten():
    """Laad ORTEC roosterdata met een specifieke query."""
    creds = dlt.secrets["sources.ortec.credentials"]

    url = URL.create(
        drivername="mssql+pyodbc",
        host=creds["host"],
        port=creds.get("port", 1433),
        database=creds["database"],
        username=creds.get("username") or None,
        password=creds.get("password") or None,
        query={"driver": creds.get("driver", "ODBC Driver 17 for SQL Server"),
               **creds.get("query", {})},
    )
    engine = create_engine(url)

    df = pd.read_sql(ORTEC_QUERY, engine)
    print(f"  Geladen: {len(df)} rijen van ORTEC")
    yield df.to_dict(orient="records")


def ingest_ortec():
    pipeline = dlt.pipeline(
        pipeline_name="ortec",
        destination="odiondataplatform",
        dataset_name="raw_ortec",
    )

    info = pipeline.run(ortec_diensten())
    print(info)


if __name__ == "__main__":
    ingest_ortec()
