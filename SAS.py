
import pandas as pd
import numpy

df=pd.read_csv("C:\\Users\\Rehmi Salma\\Desktop\\DataSet.csv")

print(df.shape)

print(df.columns)

sousdf=df[['SEQN','SMQ020', 'RIAGENDR', 'RIDAGEYR','DMDEDUC2','BMXWT', 'BMXHT', 'BMXBMI']]
print(sousdf)

print(sousdf.info())



sousdf=sousdf.rename(columns={'SEQN':'seqn',
                              'SMQ020':'smoking',
                              'RIAGENDR':'gender',
                              'RIDAGEYR':'age',
                              'DMDEDUC2':'education',
                              'BMXWT':'weight',
                              'BMXHT':'height',
                              'BMXBMI':'bmi'

                              })
print(sousdf)

print(sousdf.duplicated())

sousdf= sousdf.drop('seqn',axis=1)
print(sousdf)

print(sousdf.isnull().sum())

print(sousdf.dtypes)

sousdf['education'] = sousdf['education'].fillna(sousdf['education'].median())
sousdf['height'] = sousdf['height'].fillna(sousdf['height'].mean())
sousdf['bmi'] = sousdf['bmi'].fillna(sousdf['bmi'].mean())

print(sousdf.describe())

print(sousdf)

# Étape 1 : Sélection des colonnes numériques uniquement
colonnes_numeriques = sousdf.select_dtypes(include=['number']).columns

# Étape 2 : Création d’un masque booléen initialisé à True (on garde tout au début)
outliers_total_mask = pd.Series(True, index=sousdf.index)

# Étape 3 : Détection des outliers pour chaque colonne
for colum in colonnes_numeriques:
    Q1 = sousdf[colum].quantile(0.25)
    Q3 = sousdf[colum].quantile(0.75)
    IQR = Q3 - Q1

    seuil_bas = Q1 - 1.5 * IQR
    seuil_haut = Q3 + 1.5 * IQR

    # Création d’un masque pour la colonne (True = non-outlier)
    col_mask = (sousdf[colum] >= seuil_bas) & (sousdf[colum] <= seuil_haut)

    # Affichage des outliers détectés
    outliers_col = sousdf[~col_mask]
    print(f"Outliers pour la colonne '{colum}':")
    print(outliers_col)
    print("-" * 40)

    # Mise à jour du masque total (conserver les lignes valides partout)
    outliers_total_mask = outliers_total_mask & col_mask

# Étape 4 : Suppression des lignes contenant au moins un outlier
sousdf_clean = sousdf[outliers_total_mask].reset_index(drop=True)

# Résultat final
print(f"Nombre de lignes initiales : {len(sousdf)}")
print(f"Nombre de lignes après suppression des outliers : {len(sousdf_clean)}")


import seaborn as sns
print(sns.boxplot(sousdf))

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Construire l'URL de connexion
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Créer l'engine SQLAlchemy
engine = create_engine(DATABASE_URL)

# Nom de la table cible
table_name = "health_data_cleaned"

# Insérer les données (ex. df_sans_outliers)
try:
    df_sans_outliers.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"✅ Données insérées avec succès dans la table '{table_name}' !")
except Exception as e:
    print("❌ Erreur lors de l'insertion :", e)
