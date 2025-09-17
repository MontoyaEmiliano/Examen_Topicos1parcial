from pydantic import BaseModel
from typing import Optional

class TrackBase(BaseModel):
    track_id: str
    track_name: str
    artists: str
    album: str
    release_year: Optional[int] = None
    duration_ms: Optional[int] = None
    popularity: Optional[int] = None
    danceability: Optional[float] = None
    energy: Optional[float] = None
    valence: Optional[float] = None
    tempo: Optional[float] = None
    loudness: Optional[float] = None
    speechiness: Optional[float] = None
    acousticness: Optional[float] = None
    instrumentalness: Optional[float] = None
    liveness: Optional[float] = None

class TrackResponse(TrackBase):
    class Config:
        orm_mode = True