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

## 1/ Creation d'un virtual env sur python et sous MS DOS python -m venv .venv puis .venv\Scripts\activate.bat
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

## 5/ Lancement du serveur MLflow en local et sous MS DOS mlflow server --host 0.0.0.0 --port 8080 --backend-store-uri file:///C:/absolute_path/mlruns --default-artifact-root file:///C:/absolute_path/mlruns --serve-artifacts

mlflow server \
  --host 0.0.0.0 \
  --port 8080 \
  --backend-store-uri file:///absolute_path/mlruns \
  --default-artifact-root file:///absolute_path/mlruns \
  --serve-artifacts

-> Sur un navigateur: http://localhost:8000/

## 6/ Test de l'API
 Au moins un training doit être appelé avant de pouvoir faire un predict ou un recommend

### endpoint: load_ratings
Permet de charger les données de ratings (fichier original ratings.csv découpé en 10) pour simuler l'arrivée de nouvelle donnée. 
A noter que load_ratings fait un truncate table au départ pour éviter d'avoir des doublons et repartir d'une table propre. 
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

## 7/ Lancement de l'api dockerisée
### Pour le build et démarrage des containers: 
make all

### Pour l'arret et la suppression des containers 
make stop

### url pour tester l'api: http://localhost:8000/docs

### url pour mlflow: http://localhost:8080

### Airflow → http://localhost:8081

### Grafana → http://localhost:3000

### Prometheus → http://localhost:9090

### Airflow fonctionnement et exemple d'utilisation
📥 Fonctionnement de l’ingestion incrémentale (Airflow → FastAPI → MySQL)
Cette section décrit le fonctionnement du pipeline d’ingestion des fichiers ratings-*.csv via Airflow.
L’objectif est de simuler l’arrivée progressive de nouvelles données de notation (ratings) et de ne charger que les nouveaux fichiers, sans jamais recharger ceux déjà traités.

🎯 Objectif
Le DAG ingestion_ratings_dag implémente une ingestion incrémentale, c’est‑à‑dire :

il détecte automatiquement les fichiers présents dans data/raw/ml-20m
il identifie uniquement ceux qui n’ont pas encore été chargés
il appelle l’API FastAPI /load_ratings uniquement pour ces nouveaux fichiers
il évite tout doublon dans MySQL
il déclenche l’entraînement (training_svd_dag) uniquement si de nouvelles données ont été ingérées
📂 Où déposer les fichiers ?
Les fichiers doivent être placés dans :
data/raw/ml-20m/

🧪 Exemple concret d’utilisation
1️⃣ Premier run
Dossier :
ratings-1.csv
ratings-2.csv
Airflow charge les 2 fichiers.

2️⃣ Deuxième run
Vous ajoutez :
ratings-3.csv
ratings-4.csv
Dossier :
ratings-1.csv
ratings-2.csv
ratings-3.csv
ratings-4.csv

Airflow détecte :
Nouveaux fichiers = ["ratings-3.csv", "ratings-4.csv"]
Il ne recharge pas les fichiers 1 et 2.

3️⃣ Troisième run
Vous ajoutez :
ratings-5.csv
ratings-6.csv
Airflow charge uniquement ces deux fichiers.


### Airflow 
user:admin
pwd:admin

