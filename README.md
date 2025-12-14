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
Prerequis : installation MySQL server avec parametre local_infile = 1 dans /etc/mysql/mysql.conf.d/mysqld.cnf
Il faudra créer la base de données MySQL avec les commandes disponibles dans src/etl/create_db.sql
création d'un fichier config.yaml basé sur config.example.yaml pour saisir les identifiants de connexion

python ./src/etl/etl.py

## 4/ Lancement de l'API 
uvicorn api.api:api --app-dir src --host 0.0.0.0 --port 8000

## 5/ Lancement du serveur MLflow en local
mlflow server \
  --host 0.0.0.0 \
  --port 8080 \
  --backend-store-uri file:///absolute_path/mlruns \
  --default-artifact-root file:///absolute_path/mlruns \
  --serve-artifacts

## 6/ Test de l'API
 Au moins un training doit être appelé avant de pouvoir faire un predict ou un recommend
### endpoint: training
Pas d'input obligatoire. On peut cependant définir une limit sur le nombre de data à utiliser avec l'input "limit"
les entrainements écrivent un nouveau run dans MLflow et enregistre le modèle correspondant dans le model registry 
le meilleur modèle correspond à l'alias "best_model"
### endpoint: predict
inputs obligatoires: "user_id" et "movie_id"
utilise le modèle avec l'alias "best_model" pour faire la prédiction 
### endpoint: recommend
inputs obligatoires: "user_id" et "n_recommendations" 
renvoi une liste de recommandations pour un utilisateur donné de films qu'il n'as pas encore vus, en utilisant le "best_model"

-> Sur un navigateur: http://localhost:8000/docs


