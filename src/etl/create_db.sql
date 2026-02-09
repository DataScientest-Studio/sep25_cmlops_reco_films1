
-- Creation de la base de données reco_films
CREATE DATABASE IF NOT EXISTS reco_films
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE reco_films;

-- Creation de la table Ratings
CREATE TABLE IF NOT EXISTS Ratings (
  user_id   INT NOT NULL,
  movie_id  INT NOT NULL,
  rating    FLOAT NOT NULL,
  timestamp DATETIME NOT NULL
);

-- Creation de la table Movies
CREATE TABLE IF NOT EXISTS Movies (
  movie_id    INT NOT NULL,
  title       VARCHAR(255) NOT NULL,
  genres      VARCHAR(255) NOT NULL
);

-- Chargement du fichier moves.csv contenant le nom des films et leurs genres
LOAD DATA INFILE '/var/lib/mysql-files/movies.csv'
INTO TABLE Movies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(movie_id, title, genres);


