import json
from datetime import datetime, timedelta, time

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
KW_LISTS = [RAW_KEYWORD_LIST[i : i + 5] for i in range(0, len(RAW_KEYWORD_LIST), 5)]

TREND_REQ = TrendReq(hl="en-US", tz=0)

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleTrends"


class InterestByRegion:

    table = "InterestByRegion"

    def __init__(self, start=None, end=None):
        self.start, self.end, self.time_ranges = self.get_time_range(start, end)
        self.schema = self.get_config()

    def get_config(self):
        with open(f"configs/{self.table}.json", 'r') as f:
            config = json.load(f)
        return config['schema']

    def get_time_range(self, _start, _end):
        if _start and _end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
        else:
            query = f"""
            SELECT MAX(start) AS incre
            FROM {DATASET}.{self.table}"""
            results = BQ_CLIENT.query(query).result()
            start = [dict(i) for i in results][0]['incre']
            start = datetime.combine(start, time.min)
            end = NOW.replace(hour=0, minute=0, second=0)
        hard_start = start - timedelta(days=start.weekday())
        time_ranges = []
        while hard_start < end:
            time_ranges.append(hard_start)
            hard_start += timedelta(weeks=1)
        return hard_start, time_ranges[-1], time_ranges

    def get(self):
        rows = []
        for time_range in self.time_ranges:
            start = time_range.strftime(DATE_FORMAT)
            end = (time_range + timedelta(weeks=1)).strftime(DATE_FORMAT)
            for kw_list in KW_LISTS:
                TREND_REQ.build_payload(kw_list, timeframe=f"{start} {end}")
                results = TREND_REQ.interest_by_region(
                    resolution="COUNTRY",
                    inc_low_vol=True,
                    inc_geo_code=True,
                )
                _rows = results.to_dict("records")
                _rows = [
                    {
                        "kw": key,
                        "geoCode": row['geoCode'],
                        "value": value,
                        "start": start,
                        "end": end,
                    }
                    for row in _rows
                    for key, value in row.items() if key != "geoCode"
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
            job_config=bigquery.LoadJobConfig(
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND',
                schema=self.schema
            )
        ).result()

    def update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY `key`, `geoCode`, `value`, `start`, `end`
                ORDER BY _batched_at DESC
            ) AS row_num
            FROM {DATASET}._stage_{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query)

    def run(self):
        rows = self.get()
        response = {
            "table": self.table,
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            response['output_rows'] = loads.output_rows
        return response


x = InterestByRegion()
x.run()
