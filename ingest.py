import sqlite3
import pandas as pd
import numpy as np
import os
from tqdm import tqdm

def ingest_data(csv_file: str, db_file: str = "spotify.db") -> None:
    # Leer el CSV
    print("Leyendo archivo CSV...")
    df = pd.read_csv(csv_file)
    print(f"Filas originales: {len(df)}")
    print("Columnas en el CSV:", list(df.columns))
    
    # Renombrar columnas para que coincidan con la base de datos
    df = df.rename(columns={
        'track_artist': 'artists',
        'track_popularity': 'popularity',
        'track_album_name': 'album'
    })
    
    # Extraer el año de lanzamiento de track_album_release_date
    if 'track_album_release_date' in df.columns:
        print("Extrayendo año de lanzamiento...")
        df['release_year'] = pd.to_datetime(df['track_album_release_date'], errors='coerce').dt.year
        df['release_year'] = df['release_year'].fillna(0).astype(int)
        df.loc[df['release_year'] == 0, 'release_year'] = None
    
    # Eliminar duplicados
    df.drop_duplicates(subset='track_id', keep='first', inplace=True)
    print(f"Filas después de eliminar duplicados: {len(df)}")
    
    # Manejar valores nulos
    numeric_cols = ['popularity', 'danceability', 'energy', 'valence', 
                   'tempo', 'loudness', 'speechiness', 'acousticness', 
                   'instrumentalness', 'liveness', 'duration_ms']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)
    
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
    
    # Contador para estadísticas
    inserted = 0
    skipped = 0
    errors = 0
    
    # Insertar datos con barra de progreso
    print("Insertando datos...")
    for _, row in tqdm(df.iterrows(), total=len(df)):
        try:
            # Verificar si el track_id ya existe
            cursor.execute('SELECT track_id FROM tracks WHERE track_id = ?', (row['track_id'],))
            exists = cursor.fetchone()
            
            if exists:
                skipped += 1
                continue
                
            cursor.execute('''
            INSERT INTO tracks 
            (track_id, track_name, artists, album, release_year, duration_ms, popularity,
             danceability, energy, valence, tempo, loudness, speechiness, acousticness,
             instrumentalness, liveness)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['track_id'],
                row['track_name'],
                row['artists'],
                row['album'],
                row.get('release_year', None),
                row.get('duration_ms', None),
                row.get('popularity', None),
                row.get('danceability', None),
                row.get('energy', None),
                row.get('valence', None),
                row.get('tempo', None),
                row.get('loudness', None),
                row.get('speechiness', None),
                row.get('acousticness', None),
                row.get('instrumentalness', None),
                row.get('liveness', None)
            ))
            inserted += 1
        except Exception as e:
            errors += 1
            print(f"Error insertando fila {row['track_id']}: {e}")
    
    # Hacer commit y cerrar conexión
    conn.commit()
    conn.close()
    
    # Mostrar estadísticas
    print(f"\nIngesta completada!")
    print(f"Registros insertados: {inserted}")
    print(f"Registros omitidos (duplicados): {skipped}")
    print(f"Errores: {errors}")

if __name__ == "__main__":
    csv_file = "spotify_songs.csv" 
    
    if os.path.exists(csv_file):
        # Eliminar la base de datos existente para empezar desde cero
        if os.path.exists("spotify.db"):
            os.remove("spotify.db")
            print("Base de datos anterior eliminada.")
            
        ingest_data(csv_file)
        
        # Verificar que los datos se insertaron correctamente
        conn = sqlite3.connect("spotify.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tracks")
        count = cursor.fetchone()[0]
        conn.close()
        
        print(f"\nTotal de registros en la base de datos: {count}")
        
        # Mostrar algunos registros de ejemplo
        if count > 0:
            conn = sqlite3.connect("spotify.db")
            cursor = conn.cursor()
            cursor.execute("SELECT track_id, track_name, artists FROM tracks LIMIT 5")
            sample_records = cursor.fetchall()
            conn.close()
            
            print("\nPrimeros 5 registros:")
            for record in sample_records:
                print(f"ID: {record[0]}, Canción: {record[1]}, Artista: {record[2]}")
    else:
        print(f"Archivo {csv_file} no encontrado.")