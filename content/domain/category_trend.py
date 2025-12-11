from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class CategoryTrend:
    # 카테고리별 일자 단위 트렌드 스냅샷을 저장하기 위한 도메인 모델
    category: str
    date: date
    platform: str
    video_count: Optional[int] = None
    avg_sentiment: Optional[float] = None
    avg_trend: Optional[float] = None
    avg_total_score: Optional[float] = None
    search_volume: Optional[int] = None
    rank: Optional[int] = None
