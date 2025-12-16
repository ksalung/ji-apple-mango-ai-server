import os
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
        self.repository = repository
        self.session_factory = session_factory

    def aggregate(
        self,
        as_of: Optional[date] = None,
        window_days: int = 7,
        velocity_days: Optional[int] = None,
        platform: str | None = None,
        surge_growth_threshold: float | None = None,
    ) -> dict:
        """
        - as_of: 기준 일자 (default: 오늘)
        """
        as_of = as_of or date.today()
        from_date = as_of - timedelta(days=window_days - 1)
        prev_to = as_of - timedelta(days=window_days)
        prev_from = prev_to - timedelta(days=window_days - 1)

        if velocity_days is None:
            try:
                velocity_days = int(os.getenv("TREND_VELOCITY_DAYS", "3"))
            except ValueError:
                velocity_days = 3

        surge_threshold = surge_growth_threshold
        if surge_threshold is None:
            try:
                surge_threshold = float(os.getenv("SURGE_GROWTH_THRESHOLD", "1.0"))
            except ValueError:
                surge_threshold = 1.0

        with self.session_factory() as db:
            keyword_rows = self._aggregate_keywords(db, from_date, as_of, platform, velocity_days)
            keyword_prev_rows = self._aggregate_keywords(db, prev_from, prev_to, platform, velocity_days)
            category_rows = self._aggregate_categories(db, from_date, as_of, platform, velocity_days)
            category_prev_rows = self._aggregate_categories(db, prev_from, prev_to, platform, velocity_days)
            top_trending_videos = self._select_trending_videos(
                db=db,
                from_date=from_date,
                to_date=as_of,
                platform=platform,
                velocity_days=velocity_days,
                limit=int(os.getenv("TREND_TOP_ANALYSIS_LIMIT", "30")),
            )

        keyword_rows = self._attach_growth(keyword_rows, keyword_prev_rows, key_fields=("keyword", "platform"))
        category_rows = self._attach_growth(category_rows, category_prev_rows, key_fields=("category", "platform"))

        keyword_ranked = self._apply_rank(keyword_rows)
        category_ranked = self._apply_rank(category_rows)

        for row in keyword_ranked:
            trend = KeywordTrend(
                keyword=row["keyword"],
                date=as_of,
                platform=row["platform"],
                search_volume=row["search_volume"],
                search_volume_prev=row.get("search_volume_prev"),
                video_count=row["video_count"],
                video_count_prev=row.get("video_count_prev"),
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                growth_rate=row.get("growth_rate"),
                rank=row["rank"],
            )
            self.repository.upsert_keyword_trend(trend)

        for row in category_ranked:
            trend = CategoryTrend(
                category=row["category"],
                date=as_of,
                platform=row["platform"],
                video_count=row["video_count"],
                video_count_prev=row.get("video_count_prev"),
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                search_volume=row["search_volume"],
                search_volume_prev=row.get("search_volume_prev"),
                growth_rate=row.get("growth_rate"),
                rank=row["rank"],
            )
            self.repository.upsert_category_trend(trend)

        surging_keywords = [
            row
            for row in keyword_ranked
            if row.get("growth_rate") is not None and row.get("growth_rate", 0) >= surge_threshold
        ]
        surging_categories = [
            row
            for row in category_ranked
            if row.get("growth_rate") is not None and row.get("growth_rate", 0) >= surge_threshold
        ]

        return {
            "as_of": str(as_of),
            "keyword_trend_count": len(keyword_ranked),
            "category_trend_count": len(category_ranked),
            "surging_keywords": surging_keywords,
            "surging_categories": surging_categories,
            "top_trending_videos": top_trending_videos,
        }

    def _aggregate_keywords(
        self, db, from_date: date, as_of: date, platform: str | None, velocity_days: int
    ) -> list[dict]:
        """
        키워드 기준 집계 + 스냅샷 기반 속도(조회/좋아요/댓글) 계산.
        """
        prev_anchor = as_of - timedelta(days=velocity_days)

        rows = db.execute(
            text(
                """
                SELECT
                    km.keyword,
                    v.platform,
                    COUNT(DISTINCT km.video_id) AS video_count,
                    SUM(COALESCE(curr.view_count, v.view_count, 0)) AS search_volume,
                    SUM(COALESCE(prev.view_count, 0)) AS search_volume_prev,
                    SUM(GREATEST(COALESCE(curr.view_count, v.view_count, 0) - COALESCE(prev.view_count, 0), 0)) / :velocity_days AS view_velocity,
                    SUM(GREATEST(COALESCE(curr.like_count, v.like_count, 0) - COALESCE(prev.like_count, 0), 0)) / :velocity_days AS like_velocity,
                    SUM(GREATEST(COALESCE(curr.comment_count, v.comment_count, 0) - COALESCE(prev.comment_count, 0), 0)) / :velocity_days AS comment_velocity,
                    AVG(COALESCE(vs.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(vs.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(sc.total_score, 0)) AS avg_total_score
                FROM keyword_mapping km
                JOIN video v ON v.video_id = km.video_id
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :to_date
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) curr ON true
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :prev_anchor
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) prev ON true
                WHERE COALESCE(v.published_at::date, v.crawled_at::date) BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                GROUP BY km.keyword, v.platform
                """
            ),
            {
                "from_date": from_date,
                "to_date": as_of,
                "prev_anchor": prev_anchor,
                "platform": platform,
                "velocity_days": velocity_days,
            },
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "keyword": r["keyword"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "search_volume_prev": int(r["search_volume_prev"] or 0),
                    "view_velocity": float(r["view_velocity"] or 0),
                    "like_velocity": float(r["like_velocity"] or 0),
                    "comment_velocity": float(r["comment_velocity"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _aggregate_categories(
        self, db, from_date: date, as_of: date, platform: str | None, velocity_days: int
    ) -> list[dict]:
        """
        카테고리별 트렌드 집계 + 스냅샷 기반 속도 계산.
        """
        prev_anchor = as_of - timedelta(days=velocity_days)
        # vs.category가 없을 때 YouTube category_id를 사람이 읽을 수 있는 이름으로 변환해 집계에 포함한다.
        rows = db.execute(
            text(
                """
                WITH category_named AS (
                    SELECT
                        v.video_id,
                        v.platform,
                        COALESCE(
                            vs.category,
                            CASE v.category_id
                                WHEN 1 THEN 'Film & Animation'
                                WHEN 2 THEN 'Autos & Vehicles'
                                WHEN 10 THEN 'Music'
                                WHEN 15 THEN 'Pets & Animals'
                                WHEN 17 THEN 'Sports'
                                WHEN 19 THEN 'Travel & Events'
                                WHEN 20 THEN 'Gaming'
                                WHEN 22 THEN 'People & Blogs'
                                WHEN 23 THEN 'Comedy'
                                WHEN 24 THEN 'Entertainment'
                                WHEN 25 THEN 'News'
                                WHEN 26 THEN 'Howto & Style'
                                WHEN 27 THEN 'Education'
                                WHEN 28 THEN 'Science & Technology'
                                WHEN 29 THEN 'Nonprofits & Activism'
                                ELSE 'uncategorized'
                            END
                        ) AS category,
                        v.view_count,
                        v.like_count,
                        v.comment_count,
                        v.published_at,
                        v.crawled_at,
                        vs.sentiment_score,
                        vs.trend_score,
                        sc.total_score
                    FROM video v
                    LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                    LEFT JOIN video_score sc ON sc.video_id = v.video_id
                )
                SELECT
                    c.category,
                    c.platform,
                    COUNT(DISTINCT c.video_id) AS video_count,
                    SUM(COALESCE(curr.view_count, c.view_count, 0)) AS search_volume,
                    SUM(COALESCE(prev.view_count, 0)) AS search_volume_prev,
                    SUM(GREATEST(COALESCE(curr.view_count, c.view_count, 0) - COALESCE(prev.view_count, 0), 0)) / :velocity_days AS view_velocity,
                    SUM(GREATEST(COALESCE(curr.like_count, c.like_count, 0) - COALESCE(prev.like_count, 0), 0)) / :velocity_days AS like_velocity,
                    SUM(GREATEST(COALESCE(curr.comment_count, c.comment_count, 0) - COALESCE(prev.comment_count, 0), 0)) / :velocity_days AS comment_velocity,
                    AVG(COALESCE(c.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(c.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(c.total_score, 0)) AS avg_total_score
                FROM category_named c
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = c.video_id
                      AND s.platform = c.platform
                      AND s.snapshot_date <= :to_date
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) curr ON true
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = c.video_id
                      AND s.platform = c.platform
                      AND s.snapshot_date <= :prev_anchor
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) prev ON true
                WHERE COALESCE(c.published_at::date, c.crawled_at::date) BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR c.platform = :platform)
                GROUP BY c.category, c.platform
                """
            ),
            {
                "from_date": from_date,
                "to_date": as_of,
                "prev_anchor": prev_anchor,
                "platform": platform,
                "velocity_days": velocity_days,
            },
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "category": r["category"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "search_volume_prev": int(r["search_volume_prev"] or 0),
                    "view_velocity": float(r["view_velocity"] or 0),
                    "like_velocity": float(r["like_velocity"] or 0),
                    "comment_velocity": float(r["comment_velocity"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _apply_rank(self, rows: Iterable[dict]) -> list[dict]:
        # 플랫폼별 view_velocity 우선, 다음은 search_volume 내림차순으로 랭킹 산출
        grouped: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            grouped[r["platform"]].append(r)

        ranked: list[dict] = []
        for platform, items in grouped.items():
            items_sorted = sorted(
                items,
                key=lambda x: (
                    float(x.get("view_velocity") or 0),
                    float(x.get("search_volume") or 0),
                ),
                reverse=True,
            )
            for idx, item in enumerate(items_sorted, start=1):
                item_with_rank = dict(item)
                item_with_rank["rank"] = idx
                ranked.append(item_with_rank)
        return ranked

    def _attach_growth(
        self, current_rows: list[dict], prev_rows: list[dict], key_fields: tuple[str, str]
    ) -> list[dict]:
        """
        """
        prev_map: dict[tuple[str, str], dict] = {}
        for row in prev_rows:
            key = tuple(row[k] for k in key_fields)
            prev_map[key] = row

        enriched: list[dict] = []
        for row in current_rows:
            key = tuple(row[k] for k in key_fields)
            prev = prev_map.get(key, {})
            prev_volume = int(prev.get("search_volume") or 0)
            prev_count = int(prev.get("video_count") or 0)
            base_volume = prev_volume if prev_volume > 0 else 1
            growth_rate = (row["search_volume"] - prev_volume) / base_volume if (row["search_volume"] or prev_volume) else 0

            row_enriched = dict(row)
            row_enriched["search_volume_prev"] = prev_volume
            row_enriched["video_count_prev"] = prev_count
            row_enriched["growth_rate"] = growth_rate
            enriched.append(row_enriched)
        return enriched

    def _select_trending_videos(
        self,
        db,
        from_date: date,
        to_date: date,
        platform: str | None,
        velocity_days: int,
        limit: int,
    ) -> list[dict]:
        """
        속도(조회/댓글/좋아요 증가)에 기반해 상위 트렌딩 영상을 추출한다.
        """
        prev_anchor = to_date - timedelta(days=velocity_days)
        rows = db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.channel_id,
                    v.platform,
                    vs.category,
                    COALESCE(curr.view_count, v.view_count, 0) AS view_count,
                    COALESCE(prev.view_count, 0) AS view_count_prev,
                    COALESCE(curr.like_count, v.like_count, 0) AS like_count,
                    COALESCE(prev.like_count, 0) AS like_count_prev,
                    COALESCE(curr.comment_count, v.comment_count, 0) AS comment_count,
                    COALESCE(prev.comment_count, 0) AS comment_count_prev,
                    (COALESCE(curr.view_count, v.view_count, 0) - COALESCE(prev.view_count, 0)) / :velocity_days AS view_velocity,
                    (COALESCE(curr.like_count, v.like_count, 0) - COALESCE(prev.like_count, 0)) / :velocity_days AS like_velocity,
                    (COALESCE(curr.comment_count, v.comment_count, 0) - COALESCE(prev.comment_count, 0)) / :velocity_days AS comment_velocity,
                    COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, 0) AS total_score,
                    v.published_at,
                    v.thumbnail_url
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :to_date
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) curr ON true
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :prev_anchor
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) prev ON true
                WHERE COALESCE(v.published_at::date, v.crawled_at::date) BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                ORDER BY view_velocity DESC NULLS LAST,
                         comment_velocity DESC NULLS LAST,
                         like_velocity DESC NULLS LAST,
                         total_score DESC NULLS LAST,
                         v.crawled_at DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {
                "from_date": from_date,
                "to_date": to_date,
                "prev_anchor": prev_anchor,
                "platform": platform,
                "velocity_days": velocity_days,
                "limit": limit,
            },
        ).mappings()

        return [dict(r) for r in rows]

    def _has_new_data(self, as_of: date, from_date: date, platform: str | None) -> bool:
        """
        동일 데이터에 대해 불필요하게 집계하지 않도록, 기간 내 신규 데이터 존재 여부를 확인한다.
        - 기간: published_at 없으면 crawled_at 기준 from_date ~ as_of (집계 쿼리와 동일 기준)
        - 플랫폼 필터가 있다면 동일하게 적용
        """
        with self.session_factory() as db:
            row = db.execute(
                text(
                    """
                    SELECT 1
                    FROM video v
                    WHERE COALESCE(v.published_at::date, v.crawled_at::date) BETWEEN :from_date AND :to_date
                      AND (:platform IS NULL OR v.platform = :platform)
                    LIMIT 1
                    """
                ),
                {"from_date": from_date, "to_date": as_of, "platform": platform},
            ).first()
            return row is not None
