import asyncio
import os
from typing import Any, Dict

from sqlalchemy import text

from config.database.session import SessionLocal
from config.settings import YouTubeSettings
from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl


async def run_youtube_tag_batch_once() -> Dict[str, Any]:
    """
    category_trend 테이블에서 카테고리 목록을 가져와,
    각 (category, channel_id) 그룹의 최신 게시물 해시태그(영상 tags)를 수집하는 배치의 단일 실행 진입점.

    설정 방식:
    - YOUTUBE_TAG_INCLUDE_COMMENTS: 댓글까지 함께 적재할지 여부 (true/false, 기본 false)
    - YOUTUBE_TAG_MAX_VIDEOS: 채널별로 최근 몇 개의 영상을 수집할지 (기본 10)

    동작:
    - category_trend 에서 최근 일자의 category 목록을 가져온 뒤,
      해당 category 로 분석된 영상들(video_sentiment.category)을 통해 관련 channel_id 를 찾는다.
    - 이렇게 얻은 (category, channel_id) 쌍 각각에 대해 IngestionUseCase.ingest_channel_bundle 을 호출한다.
    - Video.snippet.tags 는 IngestionUseCase 내부에서 keyword_mapping 까지 자동 반영된다.
    """
    # category_trend 기준으로 최신 일자의 카테고리별 채널 목록을 조회
    with SessionLocal() as db:
        rows = db.execute(
            text(
                """
                WITH latest AS (
                    SELECT MAX(date) AS max_date
                    FROM category_trend
                    WHERE platform = :platform
                )
                SELECT DISTINCT ct.category, v.channel_id
                FROM category_trend ct
                JOIN latest l
                  ON ct.date = l.max_date
                 AND ct.platform = :platform
                JOIN video_sentiment vs
                  ON vs.category = ct.category
                JOIN video v
                  ON v.video_id = vs.video_id
                WHERE v.channel_id IS NOT NULL
                """
            ),
            {"platform": "youtube"},
        ).mappings().all()

    category_channels: Dict[str, list[str]] = {}
    for row in rows:
        category = row["category"]
        ch_id = row["channel_id"]
        # 한 카테고리 안에서 같은 채널이 여러 번 나올 수 있으므로 중복 방지
        channels = category_channels.setdefault(category, [])
        if ch_id not in channels:
            channels.append(ch_id)

    # 현재 배치에서는 댓글 수집은 사용하지 않지만, 향후 확장을 위해 환경변수를 유지한다.
    include_comments = os.getenv("YOUTUBE_TAG_INCLUDE_COMMENTS", "false").lower() == "true"
    max_videos = int(os.getenv("YOUTUBE_TAG_MAX_VIDEOS", "10"))

    repository = ContentRepositoryImpl()
    client = YouTubeClient(YouTubeSettings())

    summary: Dict[str, Any] = {
        "total_categories": 0,
        "total_channels": 0,
        "total_videos": 0,
        "categories": {},
    }

    for category, channels in category_channels.items():
        print(f"[YOUTUBE-TAG-BATCH] category={category} | channels={len(channels)}")
        cat_video_count = 0
        cat_channels_info: list[Dict[str, Any]] = []

        for channel_id in channels:
            print(f"[YOUTUBE-TAG-BATCH] ingest channel(tags only) | category={category}, channel_id={channel_id}")
            videos = _ingest_channel_tags_only(
                client=client,
                repository=repository,
                channel_id=channel_id,
                max_videos=max_videos,
            )
            cat_video_count += len(videos)
            summary["total_videos"] += len(videos)
            summary["total_channels"] += 1
            cat_channels_info.append(
                {
                    "channel_id": channel_id,
                    "video_count": len(videos),
                }
            )

        # 카테고리 기준으로 수집된 모든 영상 태그를 집계하여 category_trend_tag 테이블에 저장
        _insert_category_trend_tags(category=category)

        summary["categories"][category] = {
            "channel_count": len(channels),
            "video_count": cat_video_count,
            "channels": cat_channels_info,
        }
        summary["total_categories"] += 1

    return summary


def _ingest_channel_tags_only(
    client: YouTubeClient,
    repository: ContentRepositoryImpl,
    channel_id: str,
    max_videos: int,
) -> list[str]:
    """
    keyword_mapping 은 건드리지 않고, 지정한 채널의 영상 메타(특히 tags)만 upsert 한다.
    sentiment / score / comments 등은 처리하지 않는다.
    """
    videos = list(client.fetch_videos(channel_id, max_results=max_videos))
    ingested_videos: list[str] = []

    for video in videos:
        video.platform = client.platform
        repository.upsert_video(video)
        ingested_videos.append(video.video_id)

    return ingested_videos


def _insert_category_trend_tags(category: str) -> None:
    """
    video 및 video_sentiment 테이블을 조인하여,
    특정 category 로 분류된 모든 영상의 태그를 모아 category_trend_tag(tags)에 삽입한다.

    - tags: 해당 카테고리의 영상들에서 수집한 고유 태그들의 콤마 구분 문자열
    - category: category_trend_tag.category 에 저장될 카테고리 식별자
    """
    with SessionLocal() as db:
        # Postgres 기준: video.tags (콤마 구분 문자열)를 분리해서 고유 태그만 모은 뒤 다시 합친다.
        tags_row = db.execute(
            text(
                """
                WITH splitted AS (
                    SELECT DISTINCT trim(tag) AS tag
                    FROM video v
                    JOIN video_sentiment vs ON vs.video_id = v.video_id
                    CROSS JOIN LATERAL unnest(string_to_array(COALESCE(v.tags, ''), ',')) AS tag
                    WHERE vs.category = :category
                      AND COALESCE(v.tags, '') <> ''
                )
                SELECT string_agg(tag, ',') AS tags
                FROM splitted
                """
            ),
            {"category": category},
        ).mappings().one_or_none()

        if not tags_row or not tags_row.get("tags"):
            # 수집된 태그가 없으면 아무 것도 기록하지 않는다.
            return

        # 동일 category 가 이미 존재하면 삭제 후 새로 삽입한다.
        db.execute(
            text("DELETE FROM category_trend_tag WHERE category = :category"),
            {"category": category},
        )

        db.execute(
            text(
                """
                INSERT INTO category_trend_tag (category, tags, create_at)
                VALUES (:category, :tags, NOW())
                """
            ),
            {
                "category": category,
                "tags": tags_row["tags"],
            },
        )
        db.commit()


async def start_youtube_tag_scheduler():
    """
    간단한 asyncio 기반 YouTube 태그 적재 스케줄러.

    - ENABLE_YOUTUBE_TAG_BATCH=true 인 경우에만 동작
    - YOUTUBE_TAG_BATCH_INTERVAL_MINUTES (기본 60분) 주기로 run_youtube_tag_batch_once 실행
    """
    if os.getenv("ENABLE_YOUTUBE_TAG_BATCH", "false").lower() != "true":
        # 비활성화된 경우 조용히 반환하여 애플리케이션 기동에 영향 주지 않음
        return

    interval_minutes = int(os.getenv("YOUTUBE_TAG_BATCH_INTERVAL_MINUTES", "60"))
    print(f"[YOUTUBE-TAG-BATCH] scheduler started | interval={interval_minutes}m")

    try:
        while True:
            try:
                print("[YOUTUBE-TAG-BATCH] run started")
                result = await run_youtube_tag_batch_once()
                print("[YOUTUBE-TAG-BATCH] run success:", result)
            except Exception as exc:  # pylint: disable=broad-except
                # 배치 한 번 실패하더라도 다음 주기에는 재시도할 수 있도록 예외를 삼킨다.
                print("[YOUTUBE-TAG-BATCH] run failed:", exc)
            await asyncio.sleep(interval_minutes * 60)
    except asyncio.CancelledError:
        print("[YOUTUBE-TAG-BATCH] scheduler stopped")
        raise


if __name__ == "__main__":
    # 수동 실행: python -m app.batch.youtube_tag_batch
    asyncio.run(run_youtube_tag_batch_once())


