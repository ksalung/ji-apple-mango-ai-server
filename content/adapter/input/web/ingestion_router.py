from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from config.settings import OpenAISettings, YouTubeSettings
from content.adapter.input.web.request.ingest_requests import IngestChannelRequest, IngestVideoRequest
from content.application.usecase.ingestion_usecase import IngestionUseCase
from content.application.usecase.sentiment_usecase import SentimentUseCase
from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from config.database.session import SessionLocal

ingestion_router = APIRouter(tags=["ingestion"])

# 한국어 주석: 저장소/감정분석/유튜브 클라이언트를 선행 구성하여 모든 유형의 콘텐츠를 받아 DB에 기록합니다.
repository = ContentRepositoryImpl()
sentiment_usecase = SentimentUseCase(OpenAISettings()) if OpenAISettings().api_key else None
ingestion_usecase = IngestionUseCase(repository, sentiment_usecase)


def resolve_platform_client(platform: str):
    """
    한국어 주석: 현재는 유튜브만 활성화되어 있으며, 다른 플랫폼은 API 키 확보 후 주석을 해제해 연동합니다.
    """
    platform = platform.lower()
    if platform == "youtube":
        return YouTubeClient(YouTubeSettings())
    # if platform == "tiktok":
    #     return TikTokClient(...)  # API 키 확보 시 활성화
    # if platform == "instagram":
    #     return InstagramClient(...)  # API 키 확보 시 활성화
    raise HTTPException(status_code=400, detail="지원하지 않는 플랫폼입니다. (현재 youtube만 사용 가능)")


@ingestion_router.post("/{platform}/channel/{channel_id}")
async def ingest_channel(platform: str, channel_id: str, request: IngestChannelRequest):
    """
    한국어 주석: 채널 단위로 가능한 모든 영상/댓글/메타데이터를 수집하여 후속 추천용 전체 데이터셋을 쌓습니다.
    새 플랫폼이 추가되더라도 platform 파라미터만 추가하면 동일 플로우로 확장 가능합니다.
    """
    client = resolve_platform_client(platform)
    try:
        result = ingestion_usecase.ingest_channel_bundle(
            client,
            channel_id,
            include_comments=request.include_comments,
            max_videos=request.max_videos,
            max_comments=request.max_comments,
        )
        return JSONResponse(result)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@ingestion_router.post("/{platform}/video/{video_id}")
async def ingest_video(platform: str, video_id: str, request: IngestVideoRequest):
    """
    한국어 주석: 단일 영상의 모든 가능한 콘텐츠(본문, 태그, 썸네일, 댓글 등)를 모아 분석·추천에 활용합니다.
    """
    client = resolve_platform_client(platform)
    try:
        result = ingestion_usecase.ingest_video(
            client,
            video_id,
            include_comments=request.include_comments,
            max_comments=request.max_comments,
        )
        return JSONResponse(result)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# Postman 호출 예시 (한국어):
# 1) 건강 확인: GET http://localhost:8000/health
# 2) 채널 수집: POST http://localhost:8000/ingestion/youtube/channel/<CHANNEL_ID>
#    Body(raw JSON):
#    {
#      "include_comments": true,
#      "max_videos": 10,
#      "max_comments": 50
#    }
# 3) 영상 수집: POST http://localhost:8000/ingestion/youtube/video/<VIDEO_ID>
#    Body(raw JSON):
#    {
#      "include_comments": true,
#      "max_comments": 50
#    }
