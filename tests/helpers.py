from datetime import date, time, datetime
from decimal import Decimal
from typing import Dict, Any, List, Tuple

from pymssql import Cursor

cursor_description = (
    ("Col_Int", 3, None, None, None, None, None),
    ("Col_BigInt", 5, None, None, None, None, None),
    ("Col_SmallInt", 3, None, None, None, None, None),
    ("Col_TinyInt", 3, None, None, None, None, None),
    ("Col_Numeric", 5, None, None, None, None, None),
    ("Col_Decimal", 5, None, None, None, None, None),
    ("Col_Float", 3, None, None, None, None, None),
    ("Col_Real", 3, None, None, None, None, None),
    ("Col_Varbinary", 2, None, None, None, None, None),
    ("Col_Binary", 2, None, None, None, None, None),
    ("Col_Text", 1, None, None, None, None, None),
    ("Col_Varchar", 1, None, None, None, None, None),
    ("Col_Char", 1, None, None, None, None, None),
    ("Col_Ntext", 1, None, None, None, None, None),
    ("Col_Nvarchar", 1, None, None, None, None, None),
    ("Col_Nchar", 1, None, None, None, None, None),
    ("Col_Date", 2, None, None, None, None, None),
    ("Col_Time1", 2, None, None, None, None, None),
    ("Col_Time2", 2, None, None, None, None, None),
    ("Col_Time3", 2, None, None, None, None, None),
    ("Col_Time4", 2, None, None, None, None, None),
    ("Col_Time5", 2, None, None, None, None, None),
    ("Col_Time6", 2, None, None, None, None, None),
    ("Col_Time7", 2, None, None, None, None, None),
    ("Col_Smalldatetime", 4, None, None, None, None, None),
    ("Col_Datetime", 4, None, None, None, None, None),
    ("Col_Datetime2", 2, None, None, None, None, None),
    ("Col_Datetimeoffset0", 2, None, None, None, None, None),
    ("Col_Datetimeoffset1", 2, None, None, None, None, None),
    ("Col_Datetimeoffset2", 2, None, None, None, None, None),
    ("Col_Datetimeoffset3", 2, None, None, None, None, None),
    ("Col_Datetimeoffset4", 2, None, None, None, None, None),
    ("Col_Datetimeoffset5", 2, None, None, None, None, None),
    ("Col_Datetimeoffset6", 2, None, None, None, None, None),
    ("Col_Datetimeoffset7", 2, None, None, None, None, None),
    ("Col_Null", 3, None, None, None, None, None),
)

cursor_row = [
    (
        1,
        Decimal("2147483648"),
        -123,
        123,
        Decimal("124"),
        Decimal("124"),
        123.55555,
        123.55554962158203,
        b"BinaryText",
        (
            b"ABCDEF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        ),
        "abc",
        "abc",
        "a                             ",
        "abc",
        "abc",
        "a                             ",
        date(2021, 7, 7),
        time(9, 49, 17, 900000),
        time(9, 49, 17, 890000),
        time(9, 49, 17, 887000),
        time(9, 49, 17, 887000),
        time(9, 49, 17, 887000),
        time(9, 49, 17, 887000),
        time(9, 49, 17, 887000),
        datetime(2021, 7, 7, 9, 49),
        datetime(2021, 7, 7, 9, 49, 17, 887000),
        datetime(2021, 7, 7, 9, 49, 17, 887000),
        b"\x00;>\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x00\xe0",
        b"\xc0\xf8.\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x01\xe0",
        b" r-\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x02\xe0",
        b"\xf0\xfc,\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x03\xe0",
        b"\xf0\xfc,\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x04\xe0",
        b"T\xfd,\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x05\xe0",
        b"T\xfd,\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x06\xe0",
        b"Q\xfd,\xf1I\x00\x00\x00^\xad\x00\x00<\x00\x07\xe0",
        None,
    )
]


class MockCursor(Cursor):
    def __init__(
        self,
        row_count: int = 100,
        description: Tuple[Tuple, ...] = None,
        row: List[Tuple[Any, ...]] = None,
    ):
        self.executions = []
        self.row_count = row_count
        self.remaining_rows = row_count
        self.description_ = description
        self.example_row_ = row

        if self.description_:
            assert len(self.description_) == len(self.example_row_[0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, operations, parameters=None):
        self.executions.append((operations, parameters))

    @property
    def description(self):
        if self.description_:
            return self.description_
        return cursor_description

    def fetchmany(self, size=10000):
        if self.row_count - size <= 0:
            return self._get_rows(self.row_count)
        else:
            self.row_count -= size
            return self._get_rows(size)

    def _get_rows(self, size: int):
        if not self.example_row_:
            return cursor_row * size
        return self.example_row_ * size


class MockConnection:
    def __init__(self, as_dict=False, **kwargs):
        self.as_dict = as_dict
        self.cursor_ = MockCursor()

    def cursor(self):
        return self.cursor

    @staticmethod
    def commit():
        pass


def generate_fake_data_query(rows=1000, null_percentage=0) -> str:
    return f"""
        DECLARE @NullRatio FLOAT = {null_percentage};
        
        SELECT
            1 Col_Int,
            2147483648 Col_BigInt,
            CAST(-123 AS SMALLINT) Col_SmallInt,
            CAST(123 AS TINYINT) Col_TinyInt,
            CAST(123.55555 AS NUMERIC) Col_Numeric,
            CAST(123.55555 AS DECIMAL) Col_Decimal,
            CAST(123.55555 AS FLOAT) Col_Float,
            CAST(123.55555 AS REAL) Col_Real,
            CONVERT(VARBINARY(MAX), 'BinaryText') Col_Varbinary,
            CONVERT(BINARY(32), 'ABCDEF') Col_Binary,
            CAST('abc' AS TEXT) Col_Text,
            CAST('abc' AS VARCHAR(100)) Col_Varchar,
            CAST('a' AS CHAR) Col_Char,
            CAST('abc' AS NTEXT) Col_Ntext,
            CAST('abc' AS NVARCHAR(100)) Col_Nvarchar,
            CAST('a' AS NCHAR) Col_Nchar,
            CAST(GETDATE() AS DATE) Col_Date,
            CAST(SYSDATETIMEOFFSET() AS time(1)) Col_Time1,
            CAST(SYSDATETIMEOFFSET() AS time(2)) Col_Time2,
            CAST(SYSDATETIMEOFFSET() AS time(3)) Col_Time3,
            CAST(SYSDATETIMEOFFSET() AS time(4)) Col_Time4,
            CAST(SYSDATETIMEOFFSET() AS time(5)) Col_Time5,
            CAST(SYSDATETIMEOFFSET() AS time(6)) Col_Time6,
            CAST(SYSDATETIMEOFFSET() AS time(7)) Col_Time7,
            CAST(SYSDATETIMEOFFSET() AS Smalldatetime) Col_Smalldatetime,
            CAST(SYSDATETIMEOFFSET() AS Datetime) Col_Datetime,
            CAST(SYSDATETIMEOFFSET() AS Datetime2) Col_Datetime2,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(0)) Col_Datetimeoffset0,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(1)) Col_Datetimeoffset1,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(2)) Col_Datetimeoffset2,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(3)) Col_Datetimeoffset3,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(4)) Col_Datetimeoffset4,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(5)) Col_Datetimeoffset5,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(6)) Col_Datetimeoffset6,
            CAST(SYSDATETIMEOFFSET() AS Datetimeoffset(7)) Col_Datetimeoffset7,
            NULL Col_Null
        INTO
            #row;
        
        WITH BaseData AS (
            SELECT * FROM #row
            UNION ALL
            SELECT
                Col_Int + 1
                , Col_BigInt
                , Col_SmallInt
                , Col_TinyInt
                , Col_Numeric
                , Col_Decimal
                , Col_Float
                , Col_Real
                , Col_Varbinary
                , Col_Binary
                , Col_Text
                , Col_Varchar
                , Col_Char
                , Col_Ntext
                , Col_Nvarchar
                , Col_Nchar
                , Col_Date
                , Col_Time1
                , Col_Time2
                , Col_Time3
                , Col_Time4
                , Col_Time5
                , Col_Time6
                , Col_Time7
                , Col_Smalldatetime
                , Col_Datetime
                , Col_Datetime2
                , Col_Datetimeoffset0
                , Col_Datetimeoffset1
                , Col_Datetimeoffset2
                , Col_Datetimeoffset3
                , Col_Datetimeoffset4
                , Col_Datetimeoffset5
                , Col_Datetimeoffset6
                , Col_Datetimeoffset7
                , Col_Null
            FROM
                BaseData
            WHERE
                Col_Int < {rows}
        )
        SELECT
            *
        INTO
            #generated
        FROM
            BaseData
        OPTION (MAXRECURSION 0);
        
        DECLARE @Id INT = 1
        WHILE @Id <= {rows}
        BEGIN
            UPDATE #generated SET
                Col_Int = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Int)
                , Col_BigInt = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_BigInt)
                , Col_SmallInt = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_SmallInt)
                , Col_TinyInt = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_TinyInt)
                , Col_Numeric = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Numeric)
                , Col_Decimal = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Decimal)
                , Col_Float = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Float)
                , Col_Real = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Real)
                , Col_Varbinary = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Varbinary)
                , Col_Binary = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Binary)
                , Col_Text = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Text)
                , Col_Varchar = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Varchar)
                , Col_Char = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Char)
                , Col_Ntext = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Ntext)
                , Col_Nvarchar = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Nvarchar)
                , Col_Nchar = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Nchar)
                , Col_Date = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Date)
                , Col_Time1 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time1)
                , Col_Time2 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time2)
                , Col_Time3 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time3)
                , Col_Time4 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time4)
                , Col_Time5 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time5)
                , Col_Time6 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time6)
                , Col_Time7 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Time7)
                , Col_Smalldatetime = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Smalldatetime)
                , Col_Datetime = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetime)
                , Col_Datetime2 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetime2)
                , Col_Datetimeoffset0 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset0)
                , Col_Datetimeoffset1 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset1)
                , Col_Datetimeoffset2 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset2)
                , Col_Datetimeoffset3 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset3)
                , Col_Datetimeoffset4 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset4)
                , Col_Datetimeoffset5 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset5)
                , Col_Datetimeoffset6 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset6)
                , Col_Datetimeoffset7 = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Datetimeoffset7)
                , Col_Null = IIF(RAND(CHECKSUM(NEWID())) <= @NullRatio, Null, Col_Null)
            WHERE Col_Int = @Id
            
            SET @Id = @Id + 1 
         
        END
        
        SELECT * FROM #generated
    """


def check_correct_types(data: Dict[str, Any], allow_null=False):
    def check_is_none(item):
        if allow_null:
            return item is None
        return False

    def do_assert(item, intended_type) -> bool:
        return isinstance(item, intended_type) or check_is_none(item)

    columns = [
        ("Col_Int", int),
        ("Col_BigInt", float),
        ("Col_SmallInt", int),
        ("Col_TinyInt", int),
        ("Col_Numeric", float),
        ("Col_Decimal", float),
        ("Col_Float", float),
        ("Col_Real", float),
        ("Col_Varbinary", bytes),
        ("Col_Binary", bytes),
        ("Col_Text", str),
        ("Col_Varchar", str),
        ("Col_Char", str),
        ("Col_Ntext", str),
        ("Col_Nvarchar", str),
        ("Col_Nchar", str),
        ("Col_Date", date),
        ("Col_Time1", time),
        ("Col_Time2", time),
        ("Col_Time3", time),
        ("Col_Time4", time),
        ("Col_Time5", time),
        ("Col_Time6", time),
        ("Col_Time7", time),
        ("Col_Smalldatetime", datetime),
        ("Col_Datetime", datetime),
        ("Col_Datetime2", datetime),
        ("Col_Datetimeoffset0", datetime),
        ("Col_Datetimeoffset1", datetime),
        ("Col_Datetimeoffset2", datetime),
        ("Col_Datetimeoffset3", datetime),
        ("Col_Datetimeoffset4", datetime),
        ("Col_Datetimeoffset5", datetime),
        ("Col_Datetimeoffset6", datetime),
        ("Col_Datetimeoffset7", datetime),
        ("Col_Null", type(None)),
    ]

    for key, target_type in columns:
        assert do_assert(data[key], target_type)

    assert data["Col_Binary"].decode("UTF8") if data["Col_Binary"] is not None else True
