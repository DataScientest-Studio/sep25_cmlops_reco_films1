import pymysql

# Paramètres de connexion
conn = pymysql.connect(
    host="localhost",
    user="reco_user",       # adapte avec ton utilisateur
    password="reco_user",   # adapte avec ton mot de passe
    database="reco_films",  # ta base
    port=3306,
    charset="utf8mb4"
)

try:
    with conn.cursor() as cursor:
        # Purge de la table Ratings
        cursor.execute("TRUNCATE TABLE Ratings;")
        conn.commit()
        print("Table Ratings purgée avec succès.")
finally:
    conn.close()
