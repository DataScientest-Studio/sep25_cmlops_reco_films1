import joblib

def predict_rating(user_id, movie_id):
    # On prédit la note pour un utilisateur et un film donnés
    svd = joblib.load("models/svd_model.joblib")
    predicted_rating = svd.predict(user_id, movie_id).est
    return predicted_rating

if __name__ == "__main__":
    user_id = 26
    movie_id = 13232
    predicted_rating = predict_rating(user_id, movie_id)
    print(f"Predicted rating for user {user_id} and movie {movie_id}: {predicted_rating}")
