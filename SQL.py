from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os

# 1. Charger les variables depuis le fichier .env
load_dotenv()

# 2. Lire les informations de connexion
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# 3. Créer l'URL de connexion
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 4. Créer un moteur SQLAlchemy
engine = create_engine(DATABASE_URL)

# 5. Insérer les données dans la table PostgreSQL
df_sans_outliers=pd.read_csv("C:\\Users\\Rehmi Salma\\Desktop\\data_finale.csv")
df_sans_outliers.to_sql("health_data_cleaned", engine, if_exists='replace', index=False)
print("✅ Données insérées avec succès dans PostgreSQL !")