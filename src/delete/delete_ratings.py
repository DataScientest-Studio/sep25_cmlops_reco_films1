from sqlalchemy import create_engine, text

def purge_ratings_table():
    engine = create_engine("mysql+pymysql://reco_user:reco_user@localhost:3306/reco_films")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE Ratings;"))
        print("Table Ratings purgée avec succès.")

# Exemple d’utilisation
if __name__ == "__main__":
    purge_ratings_table()
