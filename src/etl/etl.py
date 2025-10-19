import sqlite3
import pandas as pd
import os
from pathlib import Path
import time


if __name__ == "__main__":

    start_time = time.time()

    # On supprime le fichier de base de données s'il existe déjà
    db_path = Path("data/database.sqlite")
    if db_path.exists():
        os.remove(db_path)

    # On se connecte à la base de données SQLite
    conn = sqlite3.connect("data/database.sqlite")

    # On exécute le script SQL pour créer les tables nécessaires 
    with open("src/etl/create_tables.sql", "r") as f:
        sql_script = f.read()
    conn.executescript(sql_script)

    print(f"Creating tables in {time.time() - start_time} seconds.")
    # On désactive les foreign keys pour l'insertion des données et on optimise la vitesse d'insertion
    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE;")
    conn.execute("PRAGMA cache_size = -10000000;")  # augmenter la taille du cache à 10 Go

    # On lit les fichiers CSV dans le dossier data/raw et on les insère dans la base de données

    
    # on merge les fichiers movies.csv et links.csv pour ajouter la colonne imdbId dans la table Movies 
        # Movies(id, title, genres, imdb_id)
    start_time = time.time()
    df = pd.read_csv("data/raw/ml-20m/movies.csv", sep=',')
    df_2 = pd.read_csv("data/raw/ml-20m/links.csv", sep=',', dtype={"imdbId": str})
    df_2['imdbId'] = df_2['imdbId'].apply(lambda x: 'tt' + x)
    df_merge = df.merge(df_2[['movieId', 'imdbId']], how='left', on='movieId')
    df_merge = df_merge.rename(columns={"movieId": "id", "imdbId": "imdb_id"})
    df_merge.to_sql("Movies", conn, if_exists='append', index=False)
    print(f"Inserting Movies in {time.time() - start_time} seconds.")

        # Ratings(id, user_id, movie_id, rating, timestamp)
    start_time = time.time()
    df_ratings = pd.read_csv("data/raw/ml-20m/ratings.csv", sep=',')
    df_ratings = df_ratings.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    # on convertit la colonne timestamp (seconds since epoch) en datetime
    df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'], unit='s')
    df_ratings.to_sql("Ratings", conn, if_exists='append', index=False)
    print(f"Inserting Ratings in {time.time() - start_time} seconds.")

        # Tags(id, user_id, movie_id, tag, timestamp)
    start_time = time.time()
    df_tags = pd.read_csv("data/raw/ml-20m/tags.csv", sep=',')
    df_tags = df_tags.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    # on convertit la colonne timestamp (seconds since epoch) en datetime
    df_tags['timestamp'] = pd.to_datetime(df_tags['timestamp'], unit='s')
    df_tags.to_sql("Tags", conn, if_exists='append', index=False)  
    print(f"Inserting Tags in {time.time() - start_time} seconds.")

        # GenomeScores(tag_id, movie_id, relevance)
    start_time = time.time()
    df_genome_tags = pd.read_csv("data/raw/ml-20m/genome-tags.csv", sep=',')
    df_genome_scores = pd.read_csv("data/raw/ml-20m/genome-scores.csv", sep=',')
    df_merge_genome = df_genome_scores.merge(df_genome_tags, how='left', on='tagId')
    df_merge_genome = df_merge_genome.rename(columns={"movieId": "movie_id"})
    df_merge_genome = df_merge_genome.drop(columns=['tagId'])
    df_merge_genome.to_sql("GenomeScores", conn, if_exists='append', index=False)
    print(f"Inserting GenomeScores in {time.time() - start_time} seconds.")

            #  IMDBTitleBasics(id, title_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres)
    start_time = time.time()
    df_imdb_basics = pd.read_csv("data/raw/imdb/title.basics.tsv", sep='\t', na_values='\\N', dtype={'runtimeMinutes': str}, quoting=3, engine='python')
    df_imdb_basics = df_imdb_basics.rename(columns={"tconst": "id", "titleType": "title_type", "primaryTitle": "primary_title", "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "start_year", "endYear": "end_year", "runtimeMinutes": "runtime_minutes", "genres": "genres"})
    df_imdb_basics["start_year"] = pd.to_numeric(df_imdb_basics["start_year"], errors="coerce").astype("Int64")
    df_imdb_basics["end_year"] = pd.to_numeric(df_imdb_basics["end_year"], errors="coerce").astype("Int64")
    df_imdb_basics["runtime_minutes"] = pd.to_numeric(df_imdb_basics["runtime_minutes"], errors="coerce").astype("Int64")
    df_imdb_basics["is_adult"] = df_imdb_basics["is_adult"].astype("boolean")
    # On ne garde que les ids présents dans notre dataset Movies
    movies_df_unique = df_merge['imdb_id'].unique()
    df_imdb_basics = df_imdb_basics[df_imdb_basics['id'].isin(movies_df_unique)]
    df_imdb_basics.to_sql("IMDBTitleBasics", conn, if_exists='append', index=False)
    print(f"Inserting IMDBTitleBasics in {time.time() - start_time} seconds.")

            # IMDBTitlePrincipals(id, title_id, name_id, category, job, characters)
    start_time = time.time()
    df_imdb_principals = pd.read_csv("data/raw/imdb/title.principals.tsv", sep='\t', na_values='\\N')
    df_imdb_principals = df_imdb_principals.rename(columns={"tconst": "title_id", "nconst": "name_id"})
    # On supprime la colonne ordering qui n'apporte pas d'information pertinente pour notre analyse
    df_imdb_principals = df_imdb_principals.drop(columns=['ordering', 'characters'])
    df_imdb_principals = df_imdb_principals.drop_duplicates()
    # On ne garde que les ids présents dans notre dataset Movies
    df_imdb_principals = df_imdb_principals[df_imdb_principals['title_id'].isin(movies_df_unique)]
    df_imdb_principals.to_sql("IMDBTitlePrincipals", conn, if_exists='append', index=False)
    print(f"Inserting IMDBTitlePrincipals in {time.time() - start_time} seconds.")
    

        # IMDBTitleCrew(id, title_id, director, writer)
    start_time = time.time()
    df_imdb_crew = pd.read_csv("data/raw/imdb/title.crew.tsv", sep='\t', na_values='\\N')
    df_imdb_crew = df_imdb_crew.rename(columns={"tconst": "title_id"})
    # On split directors et writers. On créé une ligne par director et writer
    df_imdb_crew['directors'] = df_imdb_crew['directors'].str.split(',')
    df_imdb_crew['writers'] = df_imdb_crew['writers'].str.split(',')
    df_imdb_crew = df_imdb_crew.explode('directors').rename(columns={"directors": "director"})
    df_imdb_crew = df_imdb_crew.explode('writers').rename(columns={"writers": "writer"})
    # On supprime les lignes où director et writer sont tous les deux NaN
    df_imdb_crew = df_imdb_crew.dropna(subset=['director', 'writer'], how='all')
    # On supprime les doublons
    df_imdb_crew = df_imdb_crew.drop_duplicates()
    # On ne garde que les ids présents dans notre dataset Movies
    df_imdb_crew = df_imdb_crew[df_imdb_crew['title_id'].isin(movies_df_unique)]
    df_imdb_crew.to_sql("IMDBTitleCrew", conn, if_exists='append', index=False)
    print(f"Inserting IMDBTitleCrew in {time.time() - start_time} seconds.")

         # IMDBNameBasics(id, primary_name, birth_year, death_year, primary_profession, known_for_titles)
    start_time = time.time()
    df_imdb_name_basics = pd.read_csv("data/raw/imdb/name.basics.tsv", sep='\t', na_values='\\N')
    df_imdb_name_basics = df_imdb_name_basics.rename(columns={"nconst": "id", "primaryName": "primary_name", "birthYear": "birth_year", "deathYear": "death_year", "primaryProfession": "primary_profession", "knownForTitles": "known_for_titles"})
    df_imdb_name_basics["birth_year"] = pd.to_numeric(df_imdb_name_basics["birth_year"], errors="coerce").astype("Int64")
    df_imdb_name_basics["death_year"] = pd.to_numeric(df_imdb_name_basics["death_year"], errors="coerce").astype("Int64")
    # On ne garde que les ids présents dans la table IMDBTitlePrincipals et dans director et writer de la table IMDBTitleCrew
    name_ids_in_principals = df_imdb_principals['name_id'].unique()
    name_ids_in_crew_directors = df_imdb_crew['director'].dropna().unique()
    name_ids_in_crew_writers = df_imdb_crew['writer'].dropna().unique()
    name_ids_in_principals = set(name_ids_in_principals).union(set(name_ids_in_crew_directors)).union(set(name_ids_in_crew_writers))
    df_imdb_name_basics = df_imdb_name_basics[df_imdb_name_basics['id'].isin(name_ids_in_principals)]
    df_imdb_name_basics.to_sql("IMDBNameBasics", conn, if_exists='append', index=False)
    print(f"Inserting IMDBNameBasics in {time.time() - start_time} seconds.")

        # IMDBTitleRatings(id, title_id, rating, votes)
    start_time = time.time()
    df_imdb_ratings = pd.read_csv("data/raw/imdb/title.ratings.tsv", sep='\t', na_values='\\N')
    df_imdb_ratings = df_imdb_ratings.rename(columns={"tconst": "title_id", "averageRating": "rating", "numVotes": "votes"})
    # On ne garde que les ids présents dans notre dataset Movies
    df_imdb_ratings = df_imdb_ratings[df_imdb_ratings['title_id'].isin(movies_df_unique)]
    df_imdb_ratings.to_sql("IMDBTitleRatings", conn, if_exists='append', index=False)
    print(f"Inserting IMDBTitleRatings in {time.time() - start_time} seconds.")

    # On réactive les foreign keys et on remet les paramètres de la base de données en mode normal
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA foreign_key_check;")
    print("Foreign key check passed.")
    conn.execute("PRAGMA journal_mode = DELETE;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = DEFAULT;")
    conn.execute("PRAGMA locking_mode = NORMAL;")

    conn.close()
