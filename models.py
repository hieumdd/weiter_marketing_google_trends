import json
import time
from datetime import datetime, timedelta
from google.api_core.exceptions import Forbidden

from pytrends.request import TrendReq
from google.cloud import bigquery

RAW_KEYWORD_LIST = [
    "AnyDesk",
    "RemotePC",
    "TeamViewer",
    "Zoom",
    "SplashTop",
    "LogMeIn",
]
KW_PER_LIST = 5
KW_LISTS = [
    RAW_KEYWORD_LIST[i : i + KW_PER_LIST]
    for i in range(0, len(RAW_KEYWORD_LIST), KW_PER_LIST)
]

TREND_REQ = TrendReq(hl="en-US", tz=360)

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleTrends"


class InterestOverTime:
    @property
    def table(self):
        return "InterestOverTime"

    def __init__(self, geo):
        self.geo = geo
        self.end = NOW
        self.start = NOW - timedelta(days=365)
        self.schema = self.get_config()

    def get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["schema"]

    def get(self):
        start, end = [i.strftime(DATE_FORMAT) for i in [self.start, self.end]]
        rows = []
        for kw_list in KW_LISTS:
            TREND_REQ.build_payload(
                kw_list,
                timeframe=f"{start} {end}",
                geo=self.geo,
            )
            results = TREND_REQ.interest_over_time()
            if results.empty:
                _rows = []
            else:
                results = results.reset_index()
                results
                results["date"] = results["date"].apply(lambda x: x.date())
                _rows = results.to_dict("records")
                _rows = [
                    {
                        "kw": key,
                        "geoCode": self.geo,
                        "value": value,
                        "date": row["date"].strftime(DATE_FORMAT),
                    }
                    for row in _rows
                    for key, value in row.items()
                    if key not in ("isPartial", "date")
                ]
            rows.extend(_rows)
        return rows

    def transform(self, rows):
        rows = [
            {
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]
        return rows

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            num_retries=50,
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()


    def update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY `kw`, `geoName`, `geoCode`, `start`, `end`
                ORDER BY _batched_at DESC
            ) AS row_num
            FROM {DATASET}._stage_{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query)

    def run(self):
        rows = self.get()
        response = {
            "table": self.table,
            "geo": self.geo,
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            # self.update()
            response["output_rows"] = loads.output_rows
        return response
