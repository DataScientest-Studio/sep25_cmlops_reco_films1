
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