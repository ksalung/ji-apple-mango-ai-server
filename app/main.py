import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.database.session import Base, engine
from content.adapter.input.web.ingestion_router import ingestion_router
from social_oauth.adapter.input.web.google_oauth2_router import authentication_router

load_dotenv()

os.environ.setdefault("CUDA_LAUNCH_BLOCKING", "1")
os.environ.setdefault("TORCH_USE_CUDA_DSA", "1")

app = FastAPI(title="Apple Mango AI Server", version="0.1.0")

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


# test 호출
@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    # Base.metadata.create_all(bind=engine)
    uvicorn.run(app, host=host, port=port)
