from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from typing import Optional, List
import logging

from app.database import get_db
from app.models import Track
from app.schemas import TrackResponse

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/tracks",
    tags=["tracks"]
)

@router.get("/", response_model=List[TrackResponse])
def get_tracks(
    q: Optional[str] = Query(None, description="Buscar en track_name o album"),
    artist: Optional[str] = Query(None, description="Filtrar por artista"),
    year_min: Optional[int] = Query(None, description="Año mínimo de lanzamiento"),
    year_max: Optional[int] = Query(None, description="Año máximo de lanzamiento"),
    pop_min: Optional[int] = Query(None, description="Popularidad mínima"),
    dance_min: Optional[float] = Query(None, description="Danceability mínima"),
    dance_max: Optional[float] = Query(None, description="Danceability máxima"),
    energy_min: Optional[float] = Query(None, description="Energy mínima"),
    energy_max: Optional[float] = Query(None, description="Energy máxima"),
    valence_min: Optional[float] = Query(None, description="Valence mínima"),
    valence_max: Optional[float] = Query(None, description="Valence máxima"),
    tempo_min: Optional[float] = Query(None, description="Tempo mínimo"),
    tempo_max: Optional[float] = Query(None, description="Tempo máximo"),
    sort_by: Optional[str] = Query("popularity", description="Campo para ordenar (popularity, release_year, danceability, energy)"),
    order: Optional[str] = Query("desc", description="Orden (asc o desc)"),
    page: int = Query(1, description="Número de página", ge=1),
    per_page: int = Query(50, description="Resultados por página", ge=1, le=100),
    db: Session = Depends(get_db)
):
    try:
        logger.info("Iniciando consulta de tracks con paginación")
        
        # Construir consulta base
        query = db.query(Track)
        
        # Aplicar filtros
        if q:
            logger.info(f"Aplicando filtro de búsqueda: {q}")
            query = query.filter(or_(
                Track.track_name.ilike(f"%{q}%"),
                Track.album.ilike(f"%{q}%")
            ))
        
        if artist:
            logger.info(f"Filtrando por artista: {artist}")
            query = query.filter(Track.artists.ilike(f"%{artist}%"))
        
        if year_min is not None:
            query = query.filter(Track.release_year >= year_min)
        
        if year_max is not None:
            query = query.filter(Track.release_year <= year_max)
        
        if pop_min is not None:
            query = query.filter(Track.popularity >= pop_min)
        
        # Filtros de características
        if dance_min is not None:
            query = query.filter(Track.danceability >= dance_min)
        
        if dance_max is not None:
            query = query.filter(Track.danceability <= dance_max)
        
        if energy_min is not None:
            query = query.filter(Track.energy >= energy_min)
        
        if energy_max is not None:
            query = query.filter(Track.energy <= energy_max)
        
        if valence_min is not None:
            query = query.filter(Track.valence >= valence_min)
        
        if valence_max is not None:
            query = query.filter(Track.valence <= valence_max)
        
        if tempo_min is not None:
            query = query.filter(Track.tempo >= tempo_min)
        
        if tempo_max is not None:
            query = query.filter(Track.tempo <= tempo_max)
        
        # Validar y aplicar ordenamiento
        valid_sort_fields = ["popularity", "release_year", "danceability", "energy"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(status_code=400, detail=f"Campo de ordenamiento inválido. Use uno de: {valid_sort_fields}")
        
        sort_field = getattr(Track, sort_by)
        
        if order == "asc":
            query = query.order_by(sort_field.asc())
        else:
            query = query.order_by(sort_field.desc())
        
        # Calcular paginación
        total_count = query.count()
        total_pages = (total_count + per_page - 1) // per_page
        
        # Validar página solicitada
        if page > total_pages and total_pages > 0:
            raise HTTPException(
                status_code=400, 
                detail=f"Página {page} no existe. Total de páginas: {total_pages}"
            )
        
        # Aplicar paginación
        offset = (page - 1) * per_page
        query = query.offset(offset).limit(per_page)
        
        # Ejecutar consulta
        logger.info(f"Ejecutando consulta página {page} con {per_page} resultados...")
        tracks = query.all()
        
        # Agregar headers de paginación
        response_headers = {
            "X-Total-Count": str(total_count),
            "X-Total-Pages": str(total_pages),
            "X-Current-Page": str(page),
            "X-Per-Page": str(per_page),
            "X-Has-Next": str(page < total_pages),
            "X-Has-Prev": str(page > 1)
        }
        
        logger.info(f"Se encontraron {len(tracks)} tracks en la página {page}")
        
        return tracks
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en get_tracks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

# Mantén el endpoint /all pero con paginación también
@router.get("/all", response_model=List[TrackResponse])
def get_all_tracks(
    page: int = Query(1, description="Número de página", ge=1),
    per_page: int = Query(50, description="Resultados por página", ge=1, le=100),
    db: Session = Depends(get_db)
):
    try:
        logger.info("Obteniendo todos los tracks con paginación")
        
        query = db.query(Track)
        total_count = query.count()
        total_pages = (total_count + per_page - 1) // per_page
        
        # Validar página solicitada
        if page > total_pages and total_pages > 0:
            raise HTTPException(
                status_code=400, 
                detail=f"Página {page} no existe. Total de páginas: {total_pages}"
            )
        
        offset = (page - 1) * per_page
        tracks = query.offset(offset).limit(per_page).all()
        
        logger.info(f"Se encontraron {len(tracks)} tracks en la página {page}")
        return tracks
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en get_all_tracks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")