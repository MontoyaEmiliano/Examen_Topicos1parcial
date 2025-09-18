from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Track(Base):
    __tablename__ = "tracks"
    track_id = Column(String, primary_key=True, index=True)
    track_name = Column(String)
    artists = Column(String)  # Asegúrate que sea 'artists' no 'artists'
    album = Column(String)
    release_year = Column(Integer)
    duration_ms = Column(Integer)
    popularity = Column(Integer)
    danceability = Column(Float)
    energy = Column(Float)
    valence = Column(Float)
    tempo = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    acousticness = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)