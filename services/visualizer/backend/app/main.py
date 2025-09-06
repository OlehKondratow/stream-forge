import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from app.routes import data

app = FastAPI(
    title="StreamForge Visualizer",
    description="API and Frontend for financial market data visualization.",
    version="1.0.0"
)

# CORS Middleware for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(data.router, prefix="/api", tags=["data"])

# --- Static Files and Frontend Hosting ---

STATIC_FILES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "static"))

if os.path.exists(STATIC_FILES_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_FILES_DIR), name="static")

    @app.get("/{full_path:path}")
    async def serve_react_app(full_path: str):
        index_path = os.path.join(os.path.dirname(__file__), "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        else:
            return {"message": "React app not found. Please build the frontend."}
else:
    @app.get("/")
    async def root():
        return {"message": "Backend is running. Frontend is not built or mounted."}
