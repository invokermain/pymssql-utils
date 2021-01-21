from datetime import date, datetime, time
from typing import Union, Tuple, Dict

SQLParameter = Union[str, int, float, date, datetime, time]
SQLParameters = Union[Tuple[SQLParameter, ...], Dict[str, SQLParameter], SQLParameter]
