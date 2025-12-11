import os
import asyncio
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from content.adapter.input.web.ingestion_router import ingestion_router
from social_oauth.adapter.input.web.google_oauth2_router import authentication_router
from app.batch.trend_batch import start_trend_scheduler

load_dotenv()

os.environ.setdefault("CUDA_LAUNCH_BLOCKING", "1")
os.environ.setdefault("TORCH_USE_CUDA_DSA", "1")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    트랜드 분석 수치 및 순위화 배치
    """
    app.state.trend_task = asyncio.create_task(start_trend_scheduler())
    try:
        yield
    finally:
        task = getattr(app.state, "trend_task", None)
        if task:
            task.cancel()


app = FastAPI(title="Apple Mango AI Server", version="0.1.0", lifespan=lifespan)

origins_env = os.getenv("CORS_ORIGINS")
origins = [origin for origin in origins_env.split(",") if origin] if origins_env else ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(authentication_router, prefix="/authentication")
app.include_router(ingestion_router, prefix="/ingestion")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
