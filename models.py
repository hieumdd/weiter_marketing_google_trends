import json
from datetime import datetime, timedelta, time
from abc import ABCMeta, abstractmethod

from pytrends.request import TrendReq
from google.cloud import bigquery

RAW_KEYWORD_LIST = [
    # "AnyDesk",
    # "RemotePC",
    # "TeamViewer",
    "Zoom",
    # "SplashTop",
    # "LogMeIn",
]
KW_LISTS = [RAW_KEYWORD_LIST[i : i + 5] for i in range(0, len(RAW_KEYWORD_LIST), 5)]

TREND_REQ = TrendReq(hl="en-US", tz=360)

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleTrends"


class GoogleTrends(metaclass=ABCMeta):
    @property
    @abstractmethod
    def table(self):
        pass

    def __init__(self, start, end):
        self.start, self.end, self.time_ranges = self.get_time_range(start, end)
        self.schema = self.get_config()

    @abstractmethod
    def get_time_range(self, start, end):
        pass

    def get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["schema"]

    @abstractmethod
    def get(self):
        pass

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
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            response["output_rows"] = loads.output_rows
        return response


class InterestByRegion(GoogleTrends):
    @property
    def table(self):
        return "InterestByRegion"

    def __init__(self, start, end):
        super().__init__(start, end)

    def get_time_range(self, _start, _end):
        if _start and _end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
        else:
            query = f"""
            SELECT MAX(start) AS incre
            FROM {DATASET}.{self.table}"""
            results = BQ_CLIENT.query(query).result()
            start = [dict(i) for i in results][0]["incre"]
            start = datetime.combine(start, time.min)
            end = NOW.replace(hour=0, minute=0, second=0)
        hard_start = start - timedelta(days=(start.weekday() + 1))
        _hard_start = hard_start
        time_ranges = []
        while _hard_start < end:
            time_ranges.append(_hard_start)
            _hard_start += timedelta(weeks=1)
        return hard_start, time_ranges[-1] + timedelta(days=6), time_ranges

    def get(self):
        rows = []
        for time_range in self.time_ranges:
            start = time_range.strftime(DATE_FORMAT)
            end = (time_range + timedelta(days=6)).strftime(DATE_FORMAT)
            for kw_list in KW_LISTS:
                TREND_REQ.build_payload(
                    kw_list,
                    timeframe=f"{start} {end}",
                )
                results = TREND_REQ.interest_by_region(
                    resolution="COUNTRY",
                    inc_low_vol=True,
                    inc_geo_code=True,
                )
                _rows = results.reset_index().to_dict("records")
                _rows = [
                    {
                        "kw": key,
                        "geoName": row["geoName"],
                        "geoCode": row["geoCode"],
                        "value": value,
                        "start": start,
                        "end": end,
                    }
                    for row in _rows
                    for key, value in row.items()
                    if key not in ("geoName", "geoCode")
                ]
                rows.extend(_rows)
        return rows


class InterestOverTime(GoogleTrends):
    @property
    def table(self):
        return "InterestOverTime"

    def __init__(self, start, end):
        super().__init__(start, end)

    def get_time_range(self, start, end):
        end = NOW
        start = end - timedelta(days=365)
        return start, end, None

    def get(self):
        start, end = [i.strftime(DATE_FORMAT) for i in [self.start, self.end]]
        rows = []
        for kw_list in KW_LISTS:
            TREND_REQ.build_payload(kw_list, timeframe=f"{start} {end}", geo="VN")
            results = TREND_REQ.interest_over_time()
            results = results.reset_index()
            results['date'] = results['date'].apply(lambda x: x.date())
            _rows = results.to_dict("records")
            _rows
            _rows = [
                {
                    "kw": key,
                    "geoCode": "VN",
                    "value": value,
                    "date": row['date'].strftime(DATE_FORMAT),
                }
                for row in _rows
                for key, value in row.items()
                if key not in ("isPartial", "date")
            ]
            _rows
            rows.extend(_rows)
        return rows
