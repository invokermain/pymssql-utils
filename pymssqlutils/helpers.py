from datetime import date, datetime, time
from typing import Dict, Optional, Tuple, Union

SQLParameter = Optional[Union[str, int, float, date, datetime, time, bool, bytes]]
SQLParameters = Union[Tuple[SQLParameter, ...], Dict[str, SQLParameter], SQLParameter]
