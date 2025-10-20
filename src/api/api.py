from fastapi import FastAPI
from model.training import train_svd_model
from model.predict import predict_rating
from model.predict import recommend_movies
from pydantic import BaseModel

api = FastAPI()

class TrainRequest(BaseModel):
    n_factors: int
    n_epochs: int
    
class PredictRequest(BaseModel):
    user_id: int
    movie_id: int

class RecommendRequest(BaseModel):
    user_id: int
    n_recommendations: int = 10

@api.post("/training")
def train_model(request: TrainRequest):
    training_time, saving_time = train_svd_model(n_factors=request.n_factors, n_epochs=request.n_epochs)
    return {"n_factors": request.n_factors, "n_epochs": request.n_epochs, "training_time": training_time, "saving_time": saving_time}

@api.post("/predict")
def predict(request: PredictRequest):
    predicted_rating, prediction_time, load_time = predict_rating(user_id=request.user_id, movie_id=request.movie_id)
    return {"user_id": request.user_id, "movie_id": request.movie_id, "predicted_rating": predicted_rating, "prediction_time": prediction_time, "load_time": load_time}

@api.post("/recommend")
def recommend(request: RecommendRequest):
    recommendations = recommend_movies(user_id=request.user_id, n_recommendations=request.n_recommendations)
    return {"user_id": request.user_id, "recommendations": recommendations}