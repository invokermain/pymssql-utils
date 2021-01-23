from datetime import date, datetime, time
from typing import Union, Tuple, Dict, Optional

SQLParameter = Optional[Union[str, int, float, date, datetime, time, bool]]
SQLParameters = Union[Tuple[SQLParameter, ...], Dict[str, SQLParameter], SQLParameter]
