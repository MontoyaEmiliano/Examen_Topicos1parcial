from fastapi import FastAPI
from app.routers import tracks, stats  # Agregar stats
from app.database import engine
from app.models import Base

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Spotify Songs API",
    description="API para consultar y analizar canciones de Spotify",
    version="1.0.0",
    openapi_tags=[
        {
            "name": "tracks",
            "description": "Operaciones con canciones de Spotify"
        },
        {
            "name": "statistics",
            "description": "Estadísticas y análisis de datos"
        }
    ]
)

app.include_router(tracks.router)
app.include_router(stats.router)  # Agregar esta línea

@app.get("/")
def read_root():
    return {"message": "Bienvenido a la API de Spotify Songs"}

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "API funcionando correctamente"}