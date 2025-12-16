from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class VideoMetricsSnapshot:
    """
    일별 영상 지표 스냅샷을 보관하는 도메인 모델입니다.
    트렌드 속도 계산 시 기준점으로 활용됩니다.
    """
    video_id: str
    platform: str
    snapshot_date: date
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
