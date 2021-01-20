from contextlib import contextmanager
import datetime as dt


class MockCursor:
    def __init__(self, as_dict):
        self.as_dict = as_dict

    def execute(self, *args, **kwargs):
        pass

    def executemany(self, *args, **kwargs):
        pass

    def fetchall(self):
        if self.as_dict:
            return [
                {
                    "Col_Date": "2021-01-18",
                    "Col_Time0": "22:07:47",
                    "Col_Time1": "22:07:47.1",
                    "Col_Time2": "22:07:47.11",
                    "Col_Time3": "22:07:47.113",
                    "Col_Time4": "22:07:47.1128",
                    "Col_Time5": "22:07:47.11285",
                    "Col_Time6": "22:07:47.112848",
                    "Col_Time7": "22:07:47.1128480",
                    "Col_Smalldatetime": dt.datetime(2021, 1, 18, 22, 8),
                    "Col_Datetime": dt.datetime(2021, 1, 18, 22, 7, 47, 113000),
                    "Col_Datetime2": "2021-01-18 22:07:47.1128480",
                    "Col_Datetimeoffset0": "2021-01-18 22:07:47 +00:00",
                    "Col_Datetimeoffset1": "2021-01-18 22:07:47.1 +00:00",
                    "Col_Datetimeoffset2": "2021-01-18 22:07:47.11 +00:00",
                    "Col_Datetimeoffset3": "2021-01-18 22:07:47.113 +00:00",
                    "Col_Datetimeoffset4": "2021-01-18 22:07:47.1128 +00:00",
                    "Col_Datetimeoffset5": "2021-01-18 22:07:47.11285 +00:00",
                    "Col_Datetimeoffset6": "2021-01-18 22:07:47.112848 +00:00",
                    "Col_Datetimeoffset7": "2021-01-18 22:07:47.1128480 +00:00",
                }
            ]
        else:
            return [
                (
                    "2021-01-18",
                    "22:54:04",
                    "22:54:04.2",
                    "22:54:04.18",
                    "22:54:04.180",
                    "22:54:04.1805",
                    "22:54:04.18050",
                    "22:54:04.180499",
                    "22:54:04.1804990",
                    dt.datetime(2021, 1, 18, 22, 54),
                    dt.datetime(2021, 1, 18, 22, 54, 4, 180000),
                    "2021-01-18 22:54:04.1804990",
                    "2021-01-18 22:54:04 +00:00",
                    "2021-01-18 22:54:04.2 +00:00",
                    "2021-01-18 22:54:04.18 +00:00",
                    "2021-01-18 22:54:04.180 +00:00",
                    "2021-01-18 22:54:04.1805 +00:00",
                    "2021-01-18 22:54:04.18050 +00:00",
                    "2021-01-18 22:54:04.180499 +00:00",
                    "2021-01-18 22:54:04.1804990 +00:00",
                )
            ]


class MockConnection:
    def __init__(self, as_dict=False, **kwargs):
        self.as_dict = as_dict

    @contextmanager
    def cursor(self):
        yield MockCursor(as_dict=self.as_dict)

    @staticmethod
    def commit():
        pass
