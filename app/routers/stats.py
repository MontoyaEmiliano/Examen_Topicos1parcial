from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List, Dict, Any
import statistics

from app.database import get_db
from app.models import Track

router = APIRouter(
    prefix="/stats",
    tags=["statistics"]
)

@router.get("/summary", response_model=Dict[str, Any])
def get_summary(db: Session = Depends(get_db)):
    # Número total de tracks
    n_tracks = db.query(func.count(Track.track_id)).scalar()
    
    # Número de artistas distintos
    n_artists = db.query(func.count(func.distinct(Track.artists))).scalar()
    
    # Rango de años
    min_year = db.query(func.min(Track.release_year)).filter(Track.release_year.isnot(None)).scalar()
    max_year = db.query(func.max(Track.release_year)).filter(Track.release_year.isnot(None)).scalar()
    year_range = f"{min_year}-{max_year}" if min_year and max_year else "N/A"
    
    # Media y mediana de popularidad
    popularity_values = db.query(Track.popularity).filter(Track.popularity.isnot(None)).all()
    popularity_values = [value[0] for value in popularity_values]
    
    mean_popularity = round(statistics.mean(popularity_values), 2) if popularity_values else 0
    median_popularity = round(statistics.median(popularity_values), 2) if popularity_values else 0
    
    # Top 10 artistas por número de tracks
    top_artists = db.query(
        Track.artists,
        func.count(Track.track_id).label('track_count')
    ).group_by(Track.artists).order_by(desc('track_count')).limit(10).all()
    
    top_artists_list = [{"artist": artist, "track_count": count} for artist, count in top_artists]
    
    return {
        "n_tracks": n_tracks,
        "n_artists": n_artists,
        "year_range": year_range,
        "mean_popularity": mean_popularity,
        "median_popularity": median_popularity,
        "top_10_artists": top_artists_list
    }

@router.get("/artists/top", response_model=List[Dict[str, Any]])
def get_top_artists(
    limit: int = Query(20, description="Número de artistas a retornar", ge=1, le=100),
    db: Session = Depends(get_db)
):
    top_artists = db.query(
        Track.artists,
        func.avg(Track.popularity).label('avg_popularity'),
        func.count(Track.track_id).label('track_count')
    ).filter(Track.popularity.isnot(None)).group_by(Track.artists).having(
        func.count(Track.track_id) >= 3  # Mínimo 3 tracks para ser considerado
    ).order_by(
        desc('avg_popularity'),
        desc('track_count')
    ).limit(limit).all()
    
    return [{
        "artist": artist,
        "avg_popularity": round(avg_pop, 2),
        "track_count": count
    } for artist, avg_pop, count in top_artists]