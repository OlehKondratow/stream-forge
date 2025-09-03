from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["Health"])


@router.get("/live", summary="Liveness probe")
async def live():
    return JSONResponse(content={"status": "live"})


@router.get("/ready", summary="Readiness probe")
async def ready():
    return JSONResponse(content={"status": "ready"})


@router.get("/health/startup", summary="Startup probe")
async def startup():
    return JSONResponse(content={"status": "started"})
