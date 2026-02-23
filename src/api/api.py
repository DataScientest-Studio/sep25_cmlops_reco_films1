import time
from fastapi import FastAPI, Response, Request
from model.training import train_svd_model
from model.predict import predict_rating, recommend_movies, get_best_model, load_trainset
from pydantic import BaseModel
from typing import Optional 
from typing import List 
from pydantic import Field 
import pandas as pd 
from sqlalchemy import create_engine 
from sqlalchemy import text
import yaml
import os
from prometheus_client import Counter, Histogram, generate_latest, CollectorRegistry, Gauge
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.pipeline.column_mapping import ColumnMapping

api = FastAPI()

@api.on_event("startup")
def on_startup():
    # On pré-charge le meilleur modèle et le trainset en cache
    get_best_model()
    load_trainset()
    
#--------------------------------------------Metrique Prometheus ---------------------------------- 
registry = CollectorRegistry()

# Couner pour le nombre total de requêtes API
api_requests_total = Counter(
    'api_requests_total',
    'Total number of API requests',
    ['endpoint', 'method', 'status_code'],
    registry=registry
)

# Histogram 'api_request_duration_seconds', label par endpoint, method, et status code
api_request_duration_seconds = Histogram(
    'api_request_duration_seconds',
    'API request duration in seconds',
    ['endpoint', 'method', 'status_code'],
    registry=registry
)

# Gauge 'evidently_data_drift_detected_status' pour notifier la detection de drift
# ceci permettra de voir rapidement si un drift est detecte et on pourra analyser plus en detail les rapports d'evidently
evidently_dataset_drift_detected_status = Gauge(
    'evidently_dataset_drift_detected_status',
    'Data drift detected status (1 if drift detected, 0 otherwise)',
    registry=registry
)

evidently_rating_drift_detected_status = Gauge(
    'evidently_rating_drift_detected_status',
    'Data drift detected status (1 if drift detected, 0 otherwise)',
    registry=registry
)

evidently_rating_drift_score = Gauge(
    'evidently_rating_drift_score',
    'Data drift score for ratings',
    registry=registry
)


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
    fileNames: list[str] 

#---------------------------------------------End Points---------------------------------- 


@api.post("/training")
def train_model(request: TrainRequest):
    training_time, saving_time = train_svd_model(limit=request.limit)
    # log Prometheus
    api_requests_total.labels(endpoint="/training", method="POST", status_code="200").inc()
    api_request_duration_seconds.labels(endpoint="/training", method="POST", status_code="200").observe(training_time + saving_time)
    return {"training_time": training_time, "saving_time": saving_time}


@api.post("/predict")
def predict(request: PredictRequest): 
    predicted_rating, prediction_time, load_time = predict_rating(user_id=request.user_id, movie_id=request.movie_id) 
    # log Prometheus
    api_requests_total.labels(endpoint="/predict", method="POST", status_code="200").inc()
    api_request_duration_seconds.labels(endpoint="/predict", method="POST", status_code="200").observe(prediction_time + load_time)
    return {"user_id": request.user_id, "movie_id": request.movie_id, "predicted_rating": predicted_rating, "prediction_time": prediction_time, "load_time": load_time} 


@api.post("/recommend")
def recommend(request: RecommendRequest): 
    start_time = time.time()
    recommendations = recommend_movies(user_id=request.user_id, n_recommendations=request.n_recommendations) 
    end_time = time.time()
    duration = end_time - start_time
    # log Prometheus
    api_requests_total.labels(endpoint="/recommend", method="POST", status_code="200").inc()
    api_request_duration_seconds.labels(endpoint="/recommend", method="POST", status_code="200").observe(duration)  

    return {"user_id": request.user_id, "recommendations": recommendations} 

# Connexion MySQL   
@api.post("/load_ratings") 
def load_ratings(request: LoadRequest): 
    start_time = time.time()
    loaded_files = [] 
    errors = [] 

    cfg = yaml.safe_load(open("config.yaml"))
    mysql_cfg = cfg["mysql"]
    csv_cfg = cfg["csv"]
    engine = create_engine( f"mysql+pymysql://{mysql_cfg['user']}:{mysql_cfg['password']}@{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}" ) 
    for file_name in request.fileNames:
        try: 
            path = os.path.join(csv_cfg['base_path'], file_name)
            df = pd.read_csv(path)
            df.rename(columns={ "userId": "user_id", "movieId": "movie_id" }, inplace=True)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            df.to_sql("Ratings", con=engine, if_exists="append", index=False)
            loaded_files.append(file_name)

            # Le fichier ratings-1.csv = drift_ref.csv est le fichier de référence
            # on vérifie le drift du dataset general et le drift de la colonne ratings par rapport au fichier de référence 
            reference_path = os.path.join(csv_cfg['base_path'], 'drift_ref.csv')
            reference_df = pd.read_csv(reference_path)
            reference_df.rename(columns={ "userId": "user_id", "movieId": "movie_id" }, inplace=True)
            reference_df["timestamp"] = pd.to_datetime(reference_df["timestamp"], unit="s")

            column_mapping_drift = ColumnMapping()
            column_mapping_drift.target = 'rating'
            column_mapping_drift.numerical_features = ["user_id", "movie_id", "rating"]
            

            current_report = Report(metrics=[DataDriftPreset()])
            current_report.run(reference_data=reference_df, current_data=df, column_mapping=column_mapping_drift)
            result = current_report.as_dict()["metrics"][1]["result"]

            print('report result:', result)  

            dataset_drift_detected = int(result["dataset_drift"])
            rating_drift_detected = int(result["drift_by_columns"]["rating"]["drift_detected"])

            if dataset_drift_detected:
                evidently_dataset_drift_detected_status.set(1)
            else:
                evidently_dataset_drift_detected_status.set(0)

            if rating_drift_detected:
                evidently_rating_drift_detected_status.set(1)
            else:
                evidently_rating_drift_detected_status.set(0)

            evidently_rating_drift_score.set(result["drift_by_columns"]["rating"]["drift_score"])





        except Exception as e: 
            end_time = time.time()
            duration = end_time - start_time
            # log Prometheus
            api_requests_total.labels(endpoint="/load_ratings", method="POST", status_code="500").inc()
            api_request_duration_seconds.labels(endpoint="/load_ratings", method="POST", status_code="500").observe(duration)
            errors.append({file_name: str(e)}) 
    
    end_time = time.time()
    duration = end_time - start_time
    # log Prometheus
    api_requests_total.labels(endpoint="/load_ratings", method="POST", status_code="200").inc()
    api_request_duration_seconds.labels(endpoint="/load_ratings", method="POST", status_code="200").observe(duration)


    return {
        "success": loaded_files,
        "failed": errors
    }

@api.get("/list_ratings_files")
def list_ratings_files():
    try:
        start_time = time.time()
        cfg = yaml.safe_load(open("config.yaml"))['csv']
        base_path = cfg['base_path']
        files = [f for f in os.listdir(base_path) if f.endswith(".csv") and f.startswith("ratings")]
        end_time = time.time()
        duration = end_time - start_time
        # log Prometheus
        api_requests_total.labels(endpoint="/list_ratings_files", method="GET", status_code="200").inc()
        api_request_duration_seconds.labels(endpoint="/list_ratings_files", method="GET", status_code="200").observe(duration)
        return {"available_files": files}
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        # log Prometheus
        api_requests_total.labels(endpoint="/list_ratings_files", method="GET", status_code="500").inc()
        api_request_duration_seconds.labels(endpoint="/list_ratings_files", method="GET", status_code="500").observe(duration)
        return {"error": str(e)}
    
# endpoint pour exposer les metriques Prometheus
@api.get("/metrics")
async def metrics(request: Request):
    return Response(content=generate_latest(registry), media_type="text/plain")
    
# endpoint pour truncate la table Ratings
@api.post("/truncate_ratings")
async def truncate_ratings():
    try:
        start_time = time.time()
        cfg = yaml.safe_load(open("config.yaml"))['mysql']
        engine = create_engine( f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}" ) 
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE Ratings"))
        end_time = time.time()
        duration = end_time - start_time
        # log Prometheus
        api_requests_total.labels(endpoint="/truncate_ratings", method="POST", status_code="200").inc()
        api_request_duration_seconds.labels(endpoint="/truncate_ratings", method="POST", status_code="200").observe(duration)
        return {"message": "Ratings table truncated successfully."}
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        # log Prometheus
        api_requests_total.labels(endpoint="/truncate_ratings", method="POST", status_code="500").inc()
        api_request_duration_seconds.labels(endpoint="/truncate_ratings", method="POST", status_code="500").observe(duration)
        return {"message": str(e)}
    
#endpoint pour trigger des erreurs API pour tester les metriques Prometheus
@api.get("/trigger_error")
async def trigger_error():
    try:
        start_time = time.time()
        raise ValueError("Error generated for testing Prometheus metrics")
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        # log Prometheus
        api_requests_total.labels(endpoint="/trigger_error", method="GET", status_code="500").inc()
        api_request_duration_seconds.labels(endpoint="/trigger_error", method="GET", status_code="500").observe(duration)
        return {"message": str(e)}
    
