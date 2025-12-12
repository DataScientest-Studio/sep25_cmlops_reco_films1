-- Création de la table IMDBTitleBasics
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS IMDBTitleBasics;
CREATE TABLE IMDBTitleBasics (
    id VARCHAR(100) PRIMARY KEY,
    title_type TEXT,
    primary_title TEXT,
    original_title TEXT,
    is_adult BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT
);

-- Création de la table IMDBTitleCrew
DROP TABLE IF EXISTS IMDBTitleCrew;
CREATE TABLE IMDBTitleCrew (
    id VARCHAR(100) PRIMARY KEY,
    title_id VARCHAR(100),
    director VARCHAR(100),
    writer VARCHAR(100),
    FOREIGN KEY (title_id) REFERENCES IMDBTitleBasics(id),
    FOREIGN KEY (director) REFERENCES IMDBNameBasics(id),
    FOREIGN KEY (writer) REFERENCES IMDBNameBasics(id)
);


-- Création de la table IMDBNameBasics
DROP TABLE IF EXISTS IMDBNameBasics;
CREATE TABLE IMDBNameBasics (
    id VARCHAR(100) PRIMARY KEY,
    primary_name TEXT,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession TEXT,
    known_for_titles VARCHAR(100), 
    FOREIGN KEY (known_for_titles) REFERENCES IMDBTitleBasics(id)
);

-- Création de la table IMDBTitlePrincipals
DROP TABLE IF EXISTS IMDBTitlePrincipals;
CREATE TABLE IMDBTitlePrincipals (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    title_id VARCHAR(100) NOT NULL,
    name_id VARCHAR(100) NOT NULL,
    category TEXT NOT NULL,
    job TEXT,
    FOREIGN KEY (title_id) REFERENCES IMDBTitleBasics(id),
    FOREIGN KEY (name_id) REFERENCES IMDBNameBasics(id)
);

-- Création de la table IMDBTitleRatings
DROP TABLE IF EXISTS IMDBTitleRatings;
CREATE TABLE IMDBTitleRatings (
    title_id VARCHAR(100) PRIMARY KEY,
    rating FLOAT,
    votes INTEGER,
    FOREIGN KEY (title_id) REFERENCES IMDBTitleBasics(id)
);

-- Création de la table Movies
DROP TABLE IF EXISTS Movies;
CREATE TABLE Movies (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    genres TEXT,
    imdb_id VARCHAR(100) NOT NULL, 
    FOREIGN KEY (imdb_id) REFERENCES IMDBTitleBasics(id)
    );

-- Création de la table Ratings
DROP TABLE IF EXISTS Ratings;
CREATE TABLE Ratings (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating FLOAT,
    timestamp DATETIME,
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);

-- Création de la table Tags
DROP TABLE IF EXISTS Tags;
CREATE TABLE Tags (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    tag TEXT,
    timestamp DATETIME,
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);

-- Création de la table GenomeScores
DROP TABLE IF EXISTS GenomeScores;
CREATE TABLE GenomeScores (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    movie_id INTEGER NOT NULL,
    tag TEXT,
    relevance FLOAT,
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);

SET FOREIGN_KEY_CHECKS = 1;
