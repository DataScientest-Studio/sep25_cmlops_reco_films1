import joblib
import time
import mlflow
from mlflow import MlflowClient
import yaml


def predict_rating(user_id, movie_id):
    # On prédit la note pour un utilisateur et un film donnés
    start_time = time.time()
    # On charge le meilleur modèle SVD sauvegardé depuis MLflow Model Registry
    cfg_mlflow = yaml.safe_load(open("config.yaml"))['mlflow']
    mlflow.set_tracking_uri(cfg_mlflow["tracking_uri"])
    svd = mlflow.sklearn.load_model("models:/SVD_Model@best_model")
    load_time = time.time() - start_time
    start_time = time.time()
    predicted_rating = svd.predict(user_id, movie_id).est
    prediction_time = time.time() - start_time
    return predicted_rating, prediction_time, load_time

def load_trainset():
    trainset = None
    cfg_mlflow = yaml.safe_load(open("config.yaml"))['mlflow']
    client = MlflowClient(tracking_uri=cfg_mlflow["tracking_uri"])
    best_model_version = client.get_model_version_by_alias(name="SVD_Model", alias="best_model")
    print(f"Loading trainset from best model version: {best_model_version.version}")

    best_model_run_id = best_model_version.run_id
    artifacts = client.list_artifacts(best_model_run_id)
    for artifact in artifacts:
        if artifact.path.startswith("trainset"):
            trainset_path = client.download_artifacts(best_model_run_id, artifact.path)
            trainset = joblib.load(trainset_path)
            break

    return trainset

def recommend_movies(user_id, n_recommendations=10):
    # On recommande des films à un utilisateur donné qu'il n'a pas encore vus

    # On charge le meilleur modèle SVD sauvegardé depuis MLflow Model Registry
    svd = mlflow.sklearn.load_model("models:/SVD_Model@best_model")

    # On charge le trainset sauvegardé lors de l'entrainement du modèle
    trainset = load_trainset() 
    
    # On récupère les films non vus par l'utilisateur
    all_movies = set(trainset.all_items())
    seen_movies = set(trainset.ur[trainset.to_inner_uid(user_id)])  
    non_seen_movies = list(all_movies - seen_movies)

    # On prédit les notes pour les films non vus
    predictions = []
    for movie in non_seen_movies:
        raw_movie_id = trainset.to_raw_iid(movie)
        pred = svd.predict(user_id, raw_movie_id).est
        predictions.append((movie, pred))

    # On trie les prédictions par note décroissante
    predictions.sort(key=lambda x: x[1], reverse=True)
    
    return predictions[:n_recommendations]

if __name__ == "__main__":
    user_id = 26
    movie_id = 13232
    predicted_rating, prediction_time, load_time = predict_rating(user_id, movie_id)
    print(f"Predicted rating for user {user_id} and movie {movie_id}: {predicted_rating}")
    print(f"Time taken to load model: {load_time} seconds")
    print(f"Time taken to predict rating: {prediction_time} seconds")
    print(f"Total time taken: {load_time + prediction_time} seconds")

    recommendations = recommend_movies(user_id, n_recommendations=5)
    print(f"Top 5 movie recommendations for user {user_id}:")
    for movie_id, score in recommendations:
        print(f"Movie ID: {movie_id}, Predicted Rating: {score}")