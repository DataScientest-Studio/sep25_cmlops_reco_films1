# sep25_cmlops_reco_films1

# Etape pour lancer l'API

## 0/ Téléchargement des données et les placer dans data/raw
https://grouplens.org/datasets/movielens/20m/
https://datasets.imdbws.com/

L'architecture des fichiers sera la suivante: 
data
    raw
        imdb
            name.basics.tsv
            title.akas.tsv
            title.basics.tsv
            title.crew.tsv
            title.episode.tsv
            title.principals.tsv
            title.ratings.tsv
        ml-20m
            genome-scores.csv
            genome-tags.csv
            links.csv
            movies.csv
            ratings.csv
            tags.csv

## 1/ Creation d'un virtual env sur python
python3 -m venv .venv
source .venv/bin/activate

## 2/ Installation des dépendances 
pip install -r requirements.txt

## 3/ Création de la base de données 
python ./src/etl/etl.py

## 4/ Lancement de l'entrainement pour avoir un premier modèle 
python ./src/model/training.py

## 5/ Lancement de l'API 
uvicorn api.api:api --app-dir src --host 0.0.0.0 --port 8000

## 6/ Test de l'API
Sur un navigateur: http://localhost:8000/docs


