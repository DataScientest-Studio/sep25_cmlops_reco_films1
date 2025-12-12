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

def experiment(X_train, X_test, experiment_name, run_name):

    start_time = time.time()
    svd = SVD(n_factors=10, n_epochs=3, random_state=42, verbose=True)
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
        mlflow.log_param("n_factors", 10)
        mlflow.log_param("n_epochs", 3)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("training_time", total_time)
        # On sauvegarde le modèle entraîné
        model_path = f"models/experiments/svd_model_{experiment_name}.joblib"
        joblib.dump(svd, model_path)
        mlflow.log_artifact(model_path, artifact_path="model")

if __name__ == "__main__":

    # On charge les données de notation depuis la base de données MySQL dans un DataFrame pandas en ordonnant par timestamp
    cfg = yaml.safe_load(open("config.yaml"))['mysql']
    ratings_df = pd.read_sql("SELECT user_id, movie_id, rating, timestamp FROM Ratings order by timestamp", con=f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',{cfg['port']})}/{cfg['database']}")    

    print("Number of users: ", ratings_df['user_id'].nunique())
    print("Number of movies: ", ratings_df['movie_id'].nunique())
    print("Number of ratings: ", len(ratings_df)) 

    # On simule l'arrivée de nouvelles données en divisant le dataset en 10 parties égales et en entrainant le modèle de manière incrémentale
    n_splits = 10
    split_size = len(ratings_df) // n_splits
    df = None
    for i in range(n_splits):
        start = i * split_size
        end = (i + 1) * split_size if i < n_splits - 1 else len(ratings_df)
        split_df = ratings_df.iloc[start:end]
        
        # On concatène les nouvelles données au DataFrame existant
        if df is None:
            df = split_df[['user_id', 'movie_id', 'rating']]    
        else:
            df = pd.concat([df, split_df[['user_id', 'movie_id', 'rating']]], ignore_index=True)

        reader = Reader(rating_scale=(0, 5))  
        df_surprise = Dataset.load_from_df(df[['user_id', 'movie_id', 'rating']], reader)
        X_train, X_test = train_test_split(df_surprise, test_size=0.2, random_state=42)
        experiment(X_train, X_test, "experiment_1", f"run_{i+1}")
    


