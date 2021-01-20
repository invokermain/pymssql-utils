from typing import Union, Tuple, Dict

SQLParameter = Union[str, int, float]
SQLParameters = Union[Tuple[SQLParameter, ...], Dict[str, SQLParameter], SQLParameter]
