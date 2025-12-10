from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class KeywordTrend:
    keyword: str
    date: date
    platform: str
    search_volume: Optional[int] = None
