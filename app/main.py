#examen topicos avanazados 1

#EMILIANO MONTOYA VELÁZQUEZ 197002

from fastapi import FastAPI
from app.routers import tracks
from app.database import engine
from app.models import Base

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Spotify Songs API",
    description="API para consultar y analizar canciones de Spotify",
    version="1.0.0"
)

app.include_router(tracks.router)

@app.get("/")
def read_root():
    return {"message": "Bienvenido a la API de Spotify Songs"}