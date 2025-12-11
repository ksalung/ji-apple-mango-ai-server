from collections import defaultdict
from datetime import date, timedelta
from typing import Iterable, Optional

from sqlalchemy import text

from content.application.port.content_repository_port import ContentRepositoryPort
from content.domain.category_trend import CategoryTrend
from content.domain.keyword_trend import KeywordTrend
from config.database.session import SessionLocal


class TrendAggregationUseCase:
    def __init__(self, repository: ContentRepositoryPort, session_factory=SessionLocal):
        # 주기적으로 실행되는 배치 집계를 분리한 유스케이스
        self.repository = repository
        self.session_factory = session_factory

    def aggregate(self, as_of: Optional[date] = None, window_days: int = 7, platform: str | None = None) -> dict:
        """
        최근 window_days 동안의 콘텐츠 분석 결과를 모아 카테고리/키워드 트렌드를 갱신한다.
        - as_of: 기준 일자 (default: 오늘)
        - window_days: 집계 대상 기간
        - platform: 특정 플랫폼만 집계할 때 사용 (None이면 전체)
        """
        as_of = as_of or date.today()
        from_date = as_of - timedelta(days=window_days - 1)

        with self.session_factory() as db:
            keyword_rows = self._aggregate_keywords(db, from_date, as_of, platform)
            category_rows = self._aggregate_categories(db, from_date, as_of, platform)

        # 플랫폼별 랭킹 계산 (search_volume 내림차순)
        keyword_ranked = self._apply_rank(keyword_rows)
        category_ranked = self._apply_rank(category_rows)

        # upsert 수행
        for row in keyword_ranked:
            trend = KeywordTrend(
                keyword=row["keyword"],
                date=as_of,
                platform=row["platform"],
                search_volume=row["search_volume"],
                video_count=row["video_count"],
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                rank=row["rank"],
            )
            self.repository.upsert_keyword_trend(trend)

        for row in category_ranked:
            trend = CategoryTrend(
                category=row["category"],
                date=as_of,
                platform=row["platform"],
                video_count=row["video_count"],
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                search_volume=row["search_volume"],
                rank=row["rank"],
            )
            self.repository.upsert_category_trend(trend)

        return {
            "as_of": str(as_of),
            "keyword_trend_count": len(keyword_ranked),
            "category_trend_count": len(category_ranked),
        }

    def _aggregate_keywords(self, db, from_date: date, as_of: date, platform: str | None) -> list[dict]:
        # Keyword 기준으로 영상, 점수, 조회수 등을 합산/평균
        rows = db.execute(
            text(
                """
                SELECT
                    km.keyword,
                    v.platform,
                    COUNT(DISTINCT km.video_id) AS video_count,
                    SUM(COALESCE(v.view_count, 0)) AS search_volume,
                    AVG(COALESCE(vs.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(vs.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(sc.total_score, 0)) AS avg_total_score
                FROM keyword_mapping km
                JOIN video v ON v.video_id = km.video_id
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE v.crawled_at::date BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                GROUP BY km.keyword, v.platform
                """
            ),
            {"from_date": from_date, "to_date": as_of, "platform": platform},
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "keyword": r["keyword"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _aggregate_categories(self, db, from_date: date, as_of: date, platform: str | None) -> list[dict]:
        # 카테고리별 트렌드 집계 (video_sentiment.category 기준)
        rows = db.execute(
            text(
                """
                SELECT
                    vs.category,
                    v.platform,
                    COUNT(DISTINCT vs.video_id) AS video_count,
                    SUM(COALESCE(v.view_count, 0)) AS search_volume,
                    AVG(COALESCE(vs.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(vs.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(sc.total_score, 0)) AS avg_total_score
                FROM video_sentiment vs
                JOIN video v ON v.video_id = vs.video_id
                LEFT JOIN video_score sc ON sc.video_id = vs.video_id
                WHERE v.crawled_at::date BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                GROUP BY vs.category, v.platform
                HAVING vs.category IS NOT NULL
                """
            ),
            {"from_date": from_date, "to_date": as_of, "platform": platform},
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "category": r["category"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _apply_rank(self, rows: Iterable[dict]) -> list[dict]:
        # 플랫폼별로 검색량 기준 내림차순 랭킹을 부여
        grouped: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            grouped[r["platform"]].append(r)

        ranked: list[dict] = []
        for platform, items in grouped.items():
            items_sorted = sorted(items, key=lambda x: x["search_volume"], reverse=True)
            for idx, item in enumerate(items_sorted, start=1):
                item_with_rank = dict(item)
                item_with_rank["rank"] = idx
                ranked.append(item_with_rank)
        return ranked
