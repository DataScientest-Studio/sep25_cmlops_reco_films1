import pandas as pd
import os
import yaml

# Chemin du fichier source
cfg = yaml.safe_load(open("config.yaml"))['csv']
source_file = os.path.join(cfg['base_path'], "ratings.csv")

# Dossier de sortie (même que source)
output_dir = os.path.dirname(source_file)

# Lecture du CSV complet
df = pd.read_csv(source_file)

# Nombre total de lignes
n = len(df)
print(f"Nombre total de lignes : {n}")

# Taille de chaque morceau
chunk_size = n // 10

# Découpage en 10 fichiers
for i in range(10):
    start = i * chunk_size
    # Le dernier fichier prend toutes les lignes restantes
    end = (i + 1) * chunk_size if i < 9 else n
    chunk = df.iloc[start:end]
    
    output_file = os.path.join(output_dir, f"ratings-{i+1}.csv")
    chunk.to_csv(output_file, index=False)
    print(f"Fichier créé : {output_file} ({len(chunk)} lignes)")
