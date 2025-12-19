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
import yaml
import os

api = FastAPI()

#--------------------------------------------Schemas ---------------------------------- 

class TrainRequest(BaseModel):
    limit: Optional[int] = None
    
class PredictRequest(BaseModel):
    user_id: int
    movie_id: int

class RecommendRequest(BaseModel):
    user_id: int
    n_recommendations: int = 10

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


@api.post("/training")
def train_model(request: TrainRequest):
    training_time, saving_time = train_svd_model(limit=request.limit)
    return {"training_time": training_time, "saving_time": saving_time}


@api.post("/predict")
def predict(request: PredictRequest): 
    predicted_rating, prediction_time, load_time = predict_rating(user_id=request.user_id, movie_id=request.movie_id) 
    return {"user_id": request.user_id, "movie_id": request.movie_id, "predicted_rating": predicted_rating, "prediction_time": prediction_time, "load_time": load_time} 


@api.post("/recommend")
def recommend(request: RecommendRequest): 
    recommendations = recommend_movies(user_id=request.user_id, n_recommendations=request.n_recommendations) 
    return {"user_id": request.user_id, "recommendations": recommendations} 

# Connexion MySQL   
@api.post("/load_ratings") 
def load_ratings(request: LoadRequest): 
    loaded_files = [] 
    errors = [] 

    # On truncate la table Ratings avant de charger de nouveaux fichiers
    cfg = yaml.safe_load(open("config.yaml"))['mysql'] 
    engine = create_engine( f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}" ) 
    try:
        with engine.connect() as conn:
            conn.execute("TRUNCATE TABLE Ratings;")
    except Exception as e:
        return {"error": f"Failed to truncate Ratings table: {str(e)}"}

    for file_name in request.fileNames:
        try: 
            path = os.path.join(cfg['base_path'], file_name)
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
        cfg = yaml.safe_load(open("config.yaml"))['csv']
        base_path = cfg['base_path']
        files = [f for f in os.listdir(base_path) if f.endswith(".csv") and f.startswith("ratings")]
        return {"available_files": files}
    except Exception as e:
        return {"error": str(e)}
    
