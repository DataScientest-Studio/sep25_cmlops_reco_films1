import pandas as pd
import sqlite3
from sklearn.kernel_approximation import svd
from surprise import Dataset, Reader, SVD
from surprise.model_selection import cross_validate
import time
import joblib

def train_svd_model():
    # On charge les données de notation depuis la base de données
    conn = sqlite3.connect("data/database.sqlite")
    query = "SELECT user_id, movie_id, rating FROM Ratings"
    ratings_df = pd.read_sql_query(query, conn)
    conn.close()

    print("Number of users: ", ratings_df['user_id'].nunique())
    print("Number of movies: ", ratings_df['movie_id'].nunique())
    print("Number of ratings: ", len(ratings_df))

    # On utilise la librairie Surprise pour entraîner un modèle SVD
    reader = Reader(rating_scale=(0, 5))  
    df_surprise = Dataset.load_from_df(ratings_df[['user_id', 'movie_id', 'rating']], reader)

    svd = SVD(n_factors=100, n_epochs=20, random_state=42, verbose=True)
    start_time = time.time()    
    trainset = df_surprise.build_full_trainset()
    svd.fit(trainset)
    print(f"Training SVD model in {time.time() - start_time} seconds.")

    # On sauvegarde le modèle entraîné
    joblib.dump(svd, "models/svd_model.joblib")

if __name__ == "__main__":
    train_svd_model()


