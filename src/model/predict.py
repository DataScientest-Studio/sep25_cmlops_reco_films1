import joblib
import time

def predict_rating(user_id, movie_id):
    # On prédit la note pour un utilisateur et un film donnés
    start_time = time.time()
    svd = joblib.load("models/svd_model.joblib")
    load_time = time.time() - start_time
    start_time = time.time()
    predicted_rating = svd.predict(user_id, movie_id).est
    prediction_time = time.time() - start_time
    return predicted_rating, prediction_time, load_time

def recommend_movies(user_id, n_recommendations=10):
    # On recommande des films à un utilisateur donné
    svd = joblib.load("models/svd_model.joblib")
    trainset = joblib.load("models/trainset.joblib")
    
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