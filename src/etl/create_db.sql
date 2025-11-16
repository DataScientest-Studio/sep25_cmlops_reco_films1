
-- Creation de la base de données reco_films
CREATE DATABASE IF NOT EXISTS reco_films
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- Creation d'un utilisateur reco_user avec des privileges sur la base reco_films
CREATE USER 'reco_user'@'localhost' IDENTIFIED BY 'reco_user';
GRANT ALL PRIVILEGES ON reco_films.* TO 'reco_user'@'localhost';
FLUSH PRIVILEGES;

USE reco_films;