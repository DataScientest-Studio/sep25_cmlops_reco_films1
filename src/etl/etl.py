import pandas as pd
from pathlib import Path
import time
import MySQLdb
from sqlalchemy import create_engine, text
import yaml

# Fonction pour sauvegarder un df dans un csv puis charger ce csv dans une table MySQL pour optimiser la vitesse d'insertion
def save_df_to_mysql(df, table_name, engine, columns):
    print(f"Saving DataFrame to table {table_name}...")
    start_time = time.time()
    temp_csv_path = str(Path(f"{table_name}.csv").resolve())
    df.to_csv(temp_csv_path, index=False, header=False)
    column_clause = f"({', '.join(columns)})" 
    load_statement = text(
        "LOAD DATA LOCAL INFILE '"
        f"{temp_csv_path}"
        "' INTO TABLE "
        f"{table_name} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\n' IGNORE 0 LINES "
        f"{column_clause};"
    )

    with engine.begin() as connection:
        connection.execute(load_statement)
    Path(temp_csv_path).unlink()
    print(f"Saved DataFrame to table {table_name} in {time.time() - start_time} seconds.")

if __name__ == "__main__":

    start_time = time.time()

    # Connexion à la base de données MySQL
    cfg = yaml.safe_load(Path("config.yaml").read_text())['mysql']
    conn = MySQLdb.connect(
    host=cfg['host'],
    user=cfg['user'],
    passwd=cfg['password'],
    port=cfg['port'],
    db=cfg['database'],
    charset="utf8mb4"
    )
    
    # On exécute le script SQL pour créer les tables nécessaires 
    start_time = time.time()
    with open("src/etl/create_tables.sql", encoding="utf-8") as f:
        statements = [stmt.strip() for stmt in f.read().split(";") if stmt.strip()]

    cursor = conn.cursor()
    for stmt in statements:
        cursor.execute(stmt)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Creating tables in {time.time() - start_time} seconds.")

    # On lit les fichiers CSV dans le dossier data/raw et on les insère dans la base de données
    # on merge les fichiers movies.csv et links.csv pour ajouter la colonne imdbId dans la table Movies 

    # Création de l'engine SQLAlchemy pour l'insertion avec pandas
    engine = create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',{cfg['port']})}/{cfg['database']}?local_infile=1",
        connect_args={"charset": "utf8mb4"},
    )
    # On déactive les foreign key checks pour éviter les problèmes d'insertion
    with engine.connect() as connection:
        connection.execute(text("SET FOREIGN_KEY_CHECKS=0;"))

        # Movies(id, title, genres, imdb_id)
    start_time = time.time()

    df = pd.read_csv("data/raw/ml-20m/movies.csv", sep=',')
    df_2 = pd.read_csv("data/raw/ml-20m/links.csv", sep=',', dtype={"imdbId": str})
    df_2['imdbId'] = df_2['imdbId'].apply(lambda x: 'tt' + x if pd.notna(x) else x)
    df_merge = df.merge(df_2[['movieId', 'imdbId']], how='left', on='movieId')
    df_merge = df_merge.rename(columns={"movieId": "id", "imdbId": "imdb_id"})
    columns = ['id', 'title', 'genres', 'imdb_id']
    save_df_to_mysql(df_merge[columns], "Movies", engine, columns)
    print(f"Inserting Movies in {time.time() - start_time} seconds.")

        # Ratings(id, user_id, movie_id, rating, timestamp)
    start_time = time.time()
    df_ratings = pd.read_csv("data/raw/ml-20m/ratings.csv", sep=',')
    df_ratings = df_ratings.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    # on convertit la colonne timestamp (seconds since epoch) en datetime
    df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'], unit='s')
    df_ratings = df_ratings.reset_index(drop=True)
    df_ratings['id'] = df_ratings.index + 1
    columns = ['id', 'user_id', 'movie_id', 'rating', 'timestamp']
    save_df_to_mysql(df_ratings[columns], "Ratings", engine, columns)
    print(f"Inserting Ratings in {time.time() - start_time} seconds.")

        # Tags(id, user_id, movie_id, tag, timestamp)
    start_time = time.time()
    df_tags = pd.read_csv("data/raw/ml-20m/tags.csv", sep=',')
    df_tags = df_tags.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    # on convertit la colonne timestamp (seconds since epoch) en datetime
    df_tags['timestamp'] = pd.to_datetime(df_tags['timestamp'], unit='s')
    df_tags = df_tags.reset_index(drop=True)
    df_tags['id'] = df_tags.index + 1
    columns = ['id', 'user_id', 'movie_id', 'tag', 'timestamp']
    save_df_to_mysql(df_tags[columns], "Tags", engine, columns)
    print(f"Inserting Tags in {time.time() - start_time} seconds.")

        # GenomeScores(tag_id, movie_id, relevance)
    start_time = time.time()
    df_genome_tags = pd.read_csv("data/raw/ml-20m/genome-tags.csv", sep=',')
    df_genome_scores = pd.read_csv("data/raw/ml-20m/genome-scores.csv", sep=',')
    df_merge_genome = df_genome_scores.merge(df_genome_tags, how='left', on='tagId')
    df_merge_genome = df_merge_genome.rename(columns={"movieId": "movie_id"})
    df_merge_genome = df_merge_genome.drop(columns=['tagId'])
    df_merge_genome = df_merge_genome.reset_index(drop=True)
    df_merge_genome['id'] = df_merge_genome.index + 1
    columns = ['id', 'movie_id', 'tag', 'relevance']
    save_df_to_mysql(df_merge_genome[columns], "GenomeScores", engine, columns)
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
    columns = ['id', 'title_type', 'primary_title', 'original_title', 'is_adult', 'start_year', 'end_year', 'runtime_minutes', 'genres']
    save_df_to_mysql(df_imdb_basics[columns], "IMDBTitleBasics", engine, columns)
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
    df_imdb_principals = df_imdb_principals.reset_index(drop=True)
    df_imdb_principals['id'] = df_imdb_principals.index + 1
    columns = ['id', 'title_id', 'name_id', 'category', 'job']
    save_df_to_mysql(df_imdb_principals[columns], "IMDBTitlePrincipals", engine, columns)
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
    columns = ['title_id', 'director', 'writer']
    save_df_to_mysql(df_imdb_crew[columns], "IMDBTitleCrew", engine, columns)
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
    columns = ['id', 'primary_name', 'birth_year', 'death_year', 'primary_profession', 'known_for_titles']
    save_df_to_mysql(df_imdb_name_basics[columns], "IMDBNameBasics", engine, columns)
    print(f"Inserting IMDBNameBasics in {time.time() - start_time} seconds.")

        # IMDBTitleRatings(id, title_id, rating, votes)
    start_time = time.time()
    df_imdb_ratings = pd.read_csv("data/raw/imdb/title.ratings.tsv", sep='\t', na_values='\\N')
    df_imdb_ratings = df_imdb_ratings.rename(columns={"tconst": "title_id", "averageRating": "rating", "numVotes": "votes"})
    # On ne garde que les ids présents dans notre dataset Movies
    df_imdb_ratings = df_imdb_ratings[df_imdb_ratings['title_id'].isin(movies_df_unique)]
    columns = ['title_id', 'rating', 'votes']
    save_df_to_mysql(df_imdb_ratings[columns], "IMDBTitleRatings", engine, columns)
    print(f"Inserting IMDBTitleRatings in {time.time() - start_time} seconds.")

    # On réactive les foreign key checks
    with engine.connect() as connection:
        connection.execute(text("SET FOREIGN_KEY_CHECKS=1;"))