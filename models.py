import json
import time
from datetime import datetime, timedelta
from google.api_core.exceptions import Forbidden

from pytrends.request import TrendReq
from google.cloud import bigquery

KEYWORDS = [
    "AnyDesk",
    "RemotePC",
    "TeamViewer",
    "Zoom",
    # "SplashTop",
    # "LogMeIn",
]

EST = 60 * 5
TREND_REQ = TrendReq(
    hl="en-US",
    tz=EST,
    retries=5,
    backoff_factor=10,
)

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleTrends"


class InterestOverTime:
    table = "InterestOverTime"

    @property
    def schema(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["schema"]

    def __init__(self, geo):
        self.geo = geo
        self.end = NOW
        self.start = NOW - timedelta(days=365)

    def _get(self):
        start, end = [i.strftime(DATE_FORMAT) for i in [self.start, self.end]]
        rows = []
        # for kw in KEYWORDS:
        TREND_REQ.build_payload(
            KEYWORDS,
            timeframe=f"{start} {end}",
            geo=self.geo,
        )
        results = TREND_REQ.interest_over_time()
        if results.empty:
            _rows = []
        else:
            results = results.reset_index()
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

    def _transform(self, rows):
        rows = [
            {
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]
        return rows

    def _load(self, rows):
        max_attempts = 5
        attempt = 1
        while True:
            try:
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
            except Forbidden as e:
                if attempt < max_attempts:
                    time.sleep(10 * 2 ** attempt)
                    attempt += 1
                else:
                    raise e

    def run(self):
        rows = self._get()
        response = {
            "table": self.table,
            "geo": self.geo,
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(rows)
            loads = self._load(rows)
            response["output_rows"] = loads.output_rows
        return response
