import pandas as pd
from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split
from surprise import accuracy
import time
import yaml
import mlflow
from datetime import datetime
import joblib
from mlflow.tracking import MlflowClient
import os


def set_best_model(rmse, experiment_id):
    # On détermine à partir du RMSE si c'est le meilleur modèle 
    cfg = yaml.safe_load(open("config.yaml"))['mlflow']
    client = MlflowClient(tracking_uri=cfg['tracking_uri'])

    # On récupère le run id du meilleur modèle enregistré 
    try:
        best_model_version = client.get_model_version_by_alias(name="SVD_Model", alias="best_model")
        best_model_run_id = best_model_version.run_id
         # On récupère le RMSE du meilleur modèle
        best_model_run = client.get_run(best_model_run_id)
        best_model_rmse = best_model_run.data.metrics['rmse']
    except:
        # Pour le premier run, il n'y a pas encore de modèle enregistré et une exception est levée
        best_model_run_id = None
        best_model_rmse = float('inf')

    print(f"current best model rmse: {best_model_rmse}, new model rmse: {rmse}")
    if rmse < best_model_rmse:
        # On ajoute l'alias "best_model" au nouveau modèle
        model_version = client.get_latest_versions(name="SVD_Model", stages=["None"])[-1].version
        client.set_registered_model_alias(name="SVD_Model", alias="best_model", version=model_version)




def train_svd_model(n_factors=50, n_epochs=10, limit=None):
    # On charge les données de notation depuis la base de données MySQL dans un DataFrame pandas
    start_time = time.time()
    cfg = yaml.safe_load(open("config.yaml"))['mysql']
    if limit is not None:
        ratings_df = pd.read_sql(f"SELECT user_id, movie_id, rating FROM Ratings LIMIT {limit}", con=f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',{cfg['port']})}/{cfg['database']}")    
    else:
        ratings_df = pd.read_sql("SELECT user_id, movie_id, rating FROM Ratings", con=f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',{cfg['port']})}/{cfg['database']}")    

    print("Number of users: ", ratings_df['user_id'].nunique())
    print("Number of movies: ", ratings_df['movie_id'].nunique())
    print("Number of ratings: ", len(ratings_df))

    # On utilise la librairie Surprise pour entraîner un modèle SVD
    reader = Reader(rating_scale=(0, 5))  
    df_surprise = Dataset.load_from_df(ratings_df[['user_id', 'movie_id', 'rating']], reader)

    start_time = time.time()    
    svd = SVD(n_factors=n_factors, n_epochs=n_epochs, random_state=42, verbose=True)
    X_train, X_test = train_test_split(df_surprise, test_size=0.2, random_state=42)

    svd.fit(X_train)
    total_time = time.time() - start_time
    print(f"Training SVD model in {total_time} seconds.")

    # On récupère les métriques du modèle
    preds = svd.test(X_test)
    rmse = accuracy.rmse(preds)
    mae = accuracy.mae(preds)
    mse = accuracy.mse(preds)
    print(f"RMSE: {rmse}, MAE: {mae}, MSE: {mse}")

    # On initie MLflow pour le suivi des expériences en local
    cfg_mlflow = yaml.safe_load(open("config.yaml"))['mlflow']
    mlflow.set_tracking_uri(cfg_mlflow['tracking_uri'])
    experiment = mlflow.set_experiment("SVD reco films")

    # On log un run avec la date et heure 
    current_datetime = datetime.now().strftime('%d%M%Y_%H%M%S')
    with mlflow.start_run(run_name=f"SVD_{current_datetime}"):
        # Sauvegarde des paramètres et métriques
        mlflow.log_param("n_factors", n_factors)
        mlflow.log_param("n_epochs", n_epochs)
        mlflow.log_metric("training_time", total_time)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)

        # On sauvegarde le trainset pour une utilisation ultérieure (recommandation)
        trainset = df_surprise.build_full_trainset()
        trainset_path = f"models/trainset_{current_datetime}.joblib"
        joblib.dump(trainset, trainset_path)
        mlflow.log_artifact(trainset_path)

        # On sauvegarde le modèle entraîné et les X_train, X_test
        start_time = time.time()   
        X_train_path = f"models/X_train_{current_datetime}.joblib"
        joblib.dump(X_train, X_train_path)
        X_test_path = f"models/X_test_{current_datetime}.joblib"
        joblib.dump(X_test, X_test_path)
        mlflow.log_artifact(X_train_path)
        mlflow.log_artifact(X_test_path)
        mlflow.sklearn.log_model(svd, registered_model_name="SVD_Model")
        total_time_save = time.time() - start_time
        print(f"Saving trainset in {total_time_save} seconds.")
        mlflow.log_metric("save_trainset_time", total_time_save)

        # Si c'est le meilleur modèle, on le tag comme "best_model" = "true" sinon "false"
        set_best_model(rmse, experiment.experiment_id)

        # On supprime les fichiers locaux comme ils sont poussés dans MLflow
        os.remove(trainset_path)
        os.remove(X_train_path)
        os.remove(X_test_path)



    return total_time, total_time_save

if __name__ == "__main__":
    train_svd_model(limit=5000)
    train_svd_model(limit=10000)
    train_svd_model(limit=15000)
    train_svd_model(limit=20000)
    train_svd_model(limit=25000)
    train_svd_model(limit=50000)


