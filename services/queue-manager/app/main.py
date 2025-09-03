# app/main.py
import argparse
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse # New import
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from app.logging_config import logger
from app.routes.queues import router as queue_router
from app.routes.health import router as health_router
from app.metrics.prometheus_metrics import setup_metrics

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Queue Manager started")
    yield
    # Shutdown
    logger.info("🛑 Queue Manager shutting down")

app = FastAPI(
    title="StreamForge Queue Manager",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

@app.get("/", response_class=HTMLResponse, summary="Главное меню")
async def read_root():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamForge Queue Manager</title>
        <link rel="icon" href="/docs/favicon.ico">
    </head>
    <body>
        <h1>Добро пожаловать в StreamForge Queue Manager!</h1>
        <p>Это главная страница сервиса Queue Manager.</p>
        <ul>
            <li><a href="/docs">Документация API (Swagger UI)</a></li>
            <li><a href="/redoc">Документация API (Redoc)</a></li>
            <li><a href="/health/live">Проверка живости (Liveness Probe)</a></li>
            <li><a href="/health/ready">Проверка готовности (Readiness Probe)</a></li>
        </ul>
        <p>Для получения дополнительной информации, пожалуйста, обратитесь к документации API.</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Trusted Hosts
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])

# Metrics
setup_metrics(app)

# Routers
app.include_router(health_router, prefix="/health", tags=["health"])
app.include_router(queue_router, prefix="/queues", tags=["queues"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--noop", action="store_true", help="Accepted for compatibility, but does nothing.")
    args = parser.parse_args()

    if args.noop:
        logger.info("NOOP mode enabled, but starting server anyway for CI health check.")

    uvicorn.run(app, host="0.0.0.0", port=8080)
