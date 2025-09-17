import sqlite3
import pandas as pd
import numpy as np
import os

def ingest_data(csv_file: str, db_file: str = "spotify.db") -> None:
    # Leer el CSV
    df = pd.read_csv(csv_file)
    df.drop_duplicates(subset='track_id', keep='first', inplace=True)
    
    # Manejar valores nulos
    numeric_cols = ['popularity', 'danceability', 'energy', 'valence', 
                   'tempo', 'loudness', 'speechiness', 'acousticness', 
                   'instrumentalness', 'liveness']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)
    
    # Asegurar que release_year sea entero
    if 'release_year' in df.columns:
        df['release_year'] = df['release_year'].fillna(0).astype(int)
        df.loc[df['release_year'] == 0, 'release_year'] = None
    
    # Conectar a la base de datos
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Crear tabla si no existe
    print("Creando tabla...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tracks (
        track_id TEXT PRIMARY KEY,
        track_name TEXT,
        artists TEXT,
        album TEXT,
        release_year INTEGER,
        duration_ms INTEGER,
        popularity INTEGER,
        danceability REAL,
        energy REAL,
        valence REAL,
        tempo REAL,
        loudness REAL,
        speechiness REAL,
        acousticness REAL,
        instrumentalness REAL,
        liveness REAL
    )
    ''')
    
    # Crear índices
    print("Creando índices...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_artists ON tracks(artists)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_release_year ON tracks(release_year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_popularity ON tracks(popularity)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_danceability ON tracks(danceability)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_energy ON tracks(energy)')
    
    # Insertar datos
    for _, row in df.iterrows():
        try:
            cursor.execute('''
            INSERT OR IGNORE INTO tracks 
            (track_id, track_name, artists, album, release_year, duration_ms, popularity,
             danceability, energy, valence, tempo, loudness, speechiness, acousticness,
             instrumentalness, liveness)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['track_id'],
                row['track_name'],
                row['artists'],
                row['album'],
                row['release_year'],
                row['duration_ms'],
                row['popularity'],
                row['danceability'],
                row['energy'],
                row['valence'],
                row['tempo'],
                row['loudness'],
                row['speechiness'],
                row['acousticness'],
                row['instrumentalness'],
                row['liveness']
            ))
        except Exception as e:
            print(f"Error insertando fila: {e}")
    
    # Hacer commit y cerrar conexión
    conn.commit()
    conn.close()
    print("Ingesta completada exitosamente!")

if __name__ == "__main__":
    csv_file = "spotify_songs.csv" 
    
    if os.path.exists(csv_file):
        ingest_data(csv_file)
    else:
        print(f"Archivo {csv_file} no encontrado.")

#inserta los datos del csv a la base de datos sqlite3


