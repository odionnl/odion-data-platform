from __future__ import annotations

from pathlib import Path

import dlt
import pandas as pd


@dlt.resource(name="audits", write_disposition="replace")
def ons_audits():
    """Lees audit Excel-bestanden van lokaal pad."""
    folder = dlt.secrets["ons_audits.local_path"]
    base = Path(folder)

    xlsx_files = list(base.rglob("*.xlsx"))
    print(f"  Gevonden: {len(xlsx_files)} Excel-bestanden in {folder}")

    for xlsx_path in xlsx_files:
        df = pd.read_excel(xlsx_path, sheet_name=0)
        df["source_file"] = xlsx_path.name
        print(f"  Geladen: {len(df)} rijen uit {xlsx_path.name}")
        yield df.to_dict(orient="records")


def ingest_ons_audits():
    pipeline = dlt.pipeline(
        pipeline_name="ons_audits",
        destination="odiondataplatform",
        dataset_name="raw_ons_audits",
    )

    info = pipeline.run(ons_audits())
    print(info)


if __name__ == "__main__":
    ingest_ons_audits()
