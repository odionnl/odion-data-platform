from __future__ import annotations

from pathlib import Path

import dlt
import pandas as pd


@dlt.source
def ons_audits_source(folder: str, sheet_name: str | int | None = 0):
    @dlt.resource(name="audits", write_disposition="replace")
    def audits():
        base = Path(folder)

        for xlsx_path in base.rglob("*.xlsx"):
            # Read Excel (default: first sheet)
            df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
            df = df.rename(columns={"Betreft cliënt - cliëntnummer": "clientnummer"})

            # df is a dict of {sheet: dataframe}
            if isinstance(df, dict):
                for sheet, sdf in df.items():
                    sdf["source_file"] = xlsx_path.name
                    sdf["source_sheet"] = sheet
                    yield sdf.to_dict(orient="records")
            else:
                df["source_file"] = xlsx_path.name
                yield df.to_dict(orient="records")

    return audits


def ingest_ons_audits() -> None:
    folder = dlt.secrets["ons_audits"]["local_path"]

    pipeline = dlt.pipeline(
        pipeline_name="ons_audits",
        destination="odiondataplatform",
        dataset_name="raw_ons_audits",
    )

    info = pipeline.run(
        ons_audits_source(folder),
        write_disposition="replace",  # full refresh
    )

    print(info)


if __name__ == "__main__":
    ingest_ons_audits()
