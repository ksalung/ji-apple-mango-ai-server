import asyncio
import os
from datetime import date

from content.application.usecase.trend_aggregation_usecase import TrendAggregationUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl


async def run_trend_batch_once(as_of: date | None = None, window_days: int = 7, platform: str | None = None) -> dict:
    """
    트렌드 집계를 한 번 실행하는 진입점.
    """
    usecase = TrendAggregationUseCase(ContentRepositoryImpl())
    return usecase.aggregate(as_of=as_of, window_days=window_days, platform=platform)


async def start_trend_scheduler():
    """
    간단한 asyncio 루프 기반 스케줄러.
    - 환경변수 ENABLE_TREND_BATCH=true 일 때만 구동.
    - BATCH_TREND_INTERVAL_MINUTES (기본 60), BATCH_TREND_WINDOW_DAYS (기본 7) 사용.
    """
    if os.getenv("ENABLE_TREND_BATCH", "false").lower() != "true":
        return

    interval_minutes = int(os.getenv("BATCH_TREND_INTERVAL_MINUTES", "60"))
    window_days = int(os.getenv("BATCH_TREND_WINDOW_DAYS", "7"))
    platform = os.getenv("BATCH_PLATFORM")  # 지정 플랫폼만 처리하도록 옵션(없으면 모든 플랫폼)
    # 배치 시작/종료 시점을 명확히 파악하기 위해 콘솔 로그를 남긴다.
    print(f"[TREND-BATCH] scheduler started | interval={interval_minutes}m, window_days={window_days}, platform={platform or 'all'}")
    try:
        while True:
            try:
                print("[TREND-BATCH] run started")
                result = await run_trend_batch_once(window_days=window_days, platform=platform)
                print("[TREND-BATCH] run success:", result)
            except Exception as exc:
                print("[TREND-BATCH] failed:", exc)
            await asyncio.sleep(interval_minutes * 60)
    except asyncio.CancelledError:
        # 애플리케이션 종료 시 배치가 중단된 사실을 로그로 남겨 디버깅에 활용
        print("[TREND-BATCH] scheduler stopped")
        raise


if __name__ == "__main__":
    # 수동 실행: python -m app.batch.trend_batch
    asyncio.run(run_trend_batch_once())
