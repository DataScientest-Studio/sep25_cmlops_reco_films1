import pandas as pd
from sklearn.kernel_approximation import svd
from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split
from surprise import accuracy
import time
import joblib
import yaml
from mlflow import MlflowClient
import mlflow

def experiment(n_factors, n_epochs, experiment_name, run_name):
    # On charge les données de notation depuis la base de données MySQL dans un DataFrame pandas
    start_time = time.time()
    cfg = yaml.safe_load(open("config.yaml"))['mysql']
    ratings_df = pd.read_sql("SELECT user_id, movie_id, rating FROM Ratings", con=f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',{cfg['port']})}/{cfg['database']}")    

    print("Number of users: ", ratings_df['user_id'].nunique())
    print("Number of movies: ", ratings_df['movie_id'].nunique())
    print("Number of ratings: ", len(ratings_df))

    # On utilise la librairie Surprise pour entraîner un modèle SVD
    reader = Reader(rating_scale=(0, 5))  
    df_surprise = Dataset.load_from_df(ratings_df[['user_id', 'movie_id', 'rating']], reader)

    # On divise les données en train et test
    # On ne spécifie pas de random_state pour avoir une variation entre les expériences
    X_train, X_test = train_test_split(df_surprise, test_size=0.2)

    svd = SVD(n_factors=n_factors, n_epochs=n_epochs, random_state=42, verbose=True)
    start_time = time.time()    
    svd.fit(X_train)
    total_time = time.time() - start_time
    print(f"Training SVD model in {total_time} seconds.")

    # On récupère les métriques du modèle
    preds = svd.test(X_test)
    rmse = accuracy.rmse(preds)
    mae = accuracy.mae(preds)
    mse = accuracy.mse(preds)
    print(f"RMSE: {rmse}, MAE: {mae}, MSE: {mse}")

    # On sauvegarde l'entrainement dans MLflow
    client = MlflowClient(tracking_uri="http://localhost:8080")

    mlflow.set_tracking_uri("http://localhost:8080")
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=run_name):
        mlflow.log_param("n_factors", n_factors)
        mlflow.log_param("n_epochs", n_epochs)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("training_time", total_time)
        # On sauvegarde le modèle entraîné
        model_path = f"models/experiments/svd_model_{experiment_name}.joblib"
        joblib.dump(svd, model_path)
        mlflow.log_artifact(model_path, artifact_path="model")

if __name__ == "__main__":
    experiment(50, 10, "experiment_1", "run_1")
    experiment(50, 10, "experiment_1", "run_2")
    experiment(50, 10, "experiment_1", "run_3")
    experiment(100, 20, "experiment_1", "run_4")
    experiment(80, 10, "experiment_1", "run_5")
    experiment(20, 5, "experiment_1", "run_6")


