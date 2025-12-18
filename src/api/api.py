from fastapi import FastAPI
from model.training import train_svd_model
from model.predict import predict_rating
from model.predict import recommend_movies
from pydantic import BaseModel
from typing import Optional 
from typing import List 
from pydantic import Field 
import pandas as pd 
from sqlalchemy import create_engine 
import os 
from dotenv import load_dotenv 

api = FastAPI()

class TrainRequest(BaseModel):
    limit: Optional[int] = None
    
class PredictRequest(BaseModel):
    user_id: int
    movie_id: int

class RecommendRequest(BaseModel):
    user_id: int
    n_recommendations: int = 10

@api.post("/training")
def train_model(request: TrainRequest):
    training_time, saving_time = train_svd_model(limit=request.limit)
    return {"training_time": training_time, "saving_time": saving_time}

# Charger les variables d'environnement 
load_dotenv() 

# Récupérer le chemin des données brutes depuis .env 
base_path = os.getenv("DATA_RAW_DIR") 

# Connexion MySQL 
engine = create_engine( f"mysql+pymysql://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:3306/{os.getenv('DATABASE')}" ) 

api = FastAPI() 

#--------------------------------------------Schemas ---------------------------------- 

class LoadRequest(BaseModel): 
    fileNames: list[str] = Field( 
                    default=[ "ratings-1.csv",
                               "ratings-2.csv", 
                               "ratings-3.csv", 
                               "ratings-4.csv", 
                               "ratings-5.csv", 
                               "ratings-6.csv", 
                               "ratings-7.csv", 
                               "ratings-8.csv", 
                               "ratings-9.csv", 
                               "ratings-10.csv" ], description="Liste des fichiers à charger depuis DATA_RAW_DIR" ) 

#---------------------------------------------End Points---------------------------------- 

@api.post("/predict")
def predict(request: PredictRequest): 
    predicted_rating, prediction_time, load_time = predict_rating(user_id=request.user_id, movie_id=request.movie_id) 
    return {"user_id": request.user_id, "movie_id": request.movie_id, "predicted_rating": predicted_rating, "prediction_time": prediction_time, "load_time": load_time} 


@api.post("/recommend")
def recommend(request: RecommendRequest): 
    recommendations = recommend_movies(user_id=request.user_id, n_recommendations=request.n_recommendations) 
    return {"user_id": request.user_id, "recommendations": recommendations} 

@api.post("/load_ratings") 
def load_ratings(request: LoadRequest): 
    loaded_files = [] 
    errors = [] 
    for file_name in request.fileNames:
        try: 
            path = os.path.join(base_path, file_name)
            df = pd.read_csv(path)
            df.rename(columns={ "userId": "user_id", "movieId": "movie_id" }, inplace=True)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            df.to_sql("ratings", con=engine, if_exists="append", index=False)
            loaded_files.append(file_name)
        except Exception as e: 
            errors.append({file_name: str(e)}) 
    return {
        "success": loaded_files,
        "failed": errors
}

@api.get("/list_ratings_files")
def list_ratings_files():
    try:
        files = [f for f in os.listdir(base_path) if f.endswith(".csv")]
        return {"available_files": files}
    except Exception as e:
        return {"error": str(e)}