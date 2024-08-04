from dataclasses import dataclass
from datetime import datetime


@dataclass
class RowToInsert:
    dt: datetime.date
    open: float
    high: float
    low: float
    close: float
    vol: int
