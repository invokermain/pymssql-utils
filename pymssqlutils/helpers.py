from typing import Union, Tuple, Dict
from datetime import date, datetime, time

SQLParameter = Union[str, int, float, date, datetime, time]
SQLParameters = Union[Tuple[SQLParameter, ...], Dict[str, SQLParameter], SQLParameter]
