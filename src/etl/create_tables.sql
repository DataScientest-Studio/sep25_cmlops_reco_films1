-- Création de la table IMDBTitleBasics
CREATE TABLE IMDBTitleBasics (
    id TEXT PRIMARY KEY,
    title_type TEXT,
    primary_title TEXT,
    original_title TEXT,
    is_adult BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT
);

-- Création de la table IMDBNameBasics
CREATE TABLE IMDBNameBasics (
    id TEXT PRIMARY KEY,
    primary_name TEXT,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession TEXT,
    known_for_titles TEXT, 
    FOREIGN KEY (known_for_titles) REFERENCES IMDBTitleBasics(id)
);

-- Création de la table IMDBTitlePrincipals
CREATE TABLE IMDBTitlePrincipals (
    title_id TEXT NOT NULL,
    name_id TEXT,
    category TEXT,
    job TEXT,
    PRIMARY KEY (title_id, name_id, category),
    FOREIGN KEY (title_id) REFERENCES IMDBTitleBasics(id),
    FOREIGN KEY (name_id) REFERENCES IMDBNameBasics(id)
);

-- Création de la table IMDBTitleRatings
CREATE TABLE IMDBTitleRatings (
    title_id TEXT NOT NULL,
    rating FLOAT,
    votes INTEGER,
    PRIMARY KEY (title_id),
    FOREIGN KEY (title_id) REFERENCES IMDBTitleBasics(id)
);

-- Création de la table Movies
CREATE TABLE Movies (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    genres TEXT,
    imdb_id TEXT NOT NULL, 
    FOREIGN KEY (imdb_id) REFERENCES IMDBTitleBasics(id)
    );

-- Création de la table Ratings
CREATE TABLE Ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating FLOAT,
    timestamp DATETIME,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);

-- Création de la table Tags
CREATE TABLE Tags (
    user_id INTEGER,
    movie_id INTEGER,
    tag TEXT,
    timestamp DATETIME,
    PRIMARY KEY (user_id, movie_id, tag),
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);

-- Création de la table GenomeScores
CREATE TABLE GenomeScores (
    movie_id INTEGER,
    tag TEXT,
    relevance FLOAT,
    PRIMARY KEY (movie_id, tag),
    FOREIGN KEY (movie_id) REFERENCES Movies(id)
);
