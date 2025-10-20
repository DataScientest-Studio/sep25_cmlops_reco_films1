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

if __name__ == "__main__":
    user_id = 26
    movie_id = 13232
    predicted_rating, prediction_time, load_time = predict_rating(user_id, movie_id)
    print(f"Predicted rating for user {user_id} and movie {movie_id}: {predicted_rating}")
    print(f"Time taken to load model: {load_time} seconds")
    print(f"Time taken to predict rating: {prediction_time} seconds")
    print(f"Total time taken: {load_time + prediction_time} seconds")