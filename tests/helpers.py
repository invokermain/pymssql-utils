from contextlib import contextmanager
import datetime as dt


class MockCursor:
    def execute(self, *args, **kwargs):
        pass

    def executemany(self, *args, **kwargs):
        pass

    def fetchall(self):
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


class MockConnection:
    def __init__(self, as_dict=False, **kwargs):
        self.as_dict = as_dict

    @contextmanager
    def cursor(self):
        yield MockCursor()

    @staticmethod
    def commit():
        pass
