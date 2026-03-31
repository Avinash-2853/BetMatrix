from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sys
import os

# Add the current directory to Python path for imports (since we're now in backend)
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from app.api.fetch_data_api.routes import router as fetch_data_router

# Create FastAPI app
app = FastAPI(
    title="NFL Game Prediction API",
    description="API for fetching NFL game predictions and related data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nfl-predictor.inexture.com/", "*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(fetch_data_router)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "NFL Game Prediction API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
