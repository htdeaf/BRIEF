#Analyse des Données de Santé Publique
#Charger le jeu de données à l’aide de Pandas.
import pandas as pd
import numpy as np
df=pd.read_csv("C:\\Users\\Rehmi Salma\\Desktop\\DataSet.csv")
# Afficher la taille (dimensions) du dataset (lignes, colonnes)
print(df.shape)
# Lister les colonnes disponibles dans le dataset.
print(df.columns)
# Créer un sous-ensemble du jeu de données contenant uniquement les colonnes suivantes :
# ['SEQN','SMQ020', 'RIAGENDR', 'RIDAGEYR','DMDEDUC2','BMXWT', 'BMXHT', 'BMXBMI'].
sousdf=df[['SEQN','SMQ020', 'RIAGENDR', 'RIDAGEYR','DMDEDUC2','BMXWT', 'BMXHT', 'BMXBMI']]
print(sousdf)
# Afficher les informations générales (.info()) sur ce sous-ensemble.
print(sousdf.info())
# Renommer les colonnes avec des noms plus explicites :
# ['seqn','smoking','gender', 'age','education','weight','height','bmi'].
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
# Vérifier la présence de doublons dans le dataset.
print(sousdf.duplicated())
# Supprimer la colonne 'seqn', considérée comme un identifiant inutile pour l’analyse.
sousdf= sousdf.drop('seqn',axis=1)
print(sousdf)
# Identifier les valeurs manquantes (NaN) dans les colonnes.
print(sousdf.isnull().sum())
# Remplacer les valeurs manquantes
sousdf['education'] = sousdf['education'].fillna(sousdf['education'].median())
sousdf['height'] = sousdf['height'].fillna(sousdf['height'].mean())
sousdf['bmi'] = sousdf['bmi'].fillna(sousdf['bmi'].mean())
# Afficher les statistiques descriptives
print(sousdf.describe())
# Détecter les valeurs aberrantes (outliers) à l’aide de méthodes statistiques.
for colum in sousdf.columns.to_list():
    Q1 = sousdf[colum].quantile(0.25)
    Q3 = sousdf[colum].quantile(0.75)
    IQR = Q3 - Q1

    seuil_bas = Q1 - 1.5 * IQR
    seuil_haut = Q3 + 1.5 * IQR

    df_sans_outliers = (sousdf[colum] >= seuil_bas) & (sousdf[colum] <= seuil_haut)
    print(f"Outliers pour la colonne '{colum}':")
    print(df_sans_outliers)

# Supprimer les outliers pour améliorer la qualité des données.
df_sans_outliers = sousdf.copy()

colonnes = ['smoking', 'gender', 'age', 'education', 'weight', 'height', 'bmi']

for col in colonnes:
    Q1 = df_sans_outliers[col].quantile(0.25)
    Q3 = df_sans_outliers[col].quantile(0.75)
    IQR = Q3 - Q1
    borne_inf = Q1 - 1.5 * IQR
    borne_sup = Q3 + 1.5 * IQR

    df_sans_outliers = df_sans_outliers[(df_sans_outliers[col] >= borne_inf) & (df_sans_outliers[col] <= borne_sup)]


import seaborn as sns
print(sns.boxplot(sousdf))
import seaborn as sns
print(sns.boxplot(df_sans_outliers))

# Remplacer les codes numériques par des labels explicites dans trois colonnes :
import numpy as np
smoking_dict = {1: 'yes',
                2: 'no',
                7: np.nan,
                8: np.nan}

gender_dict = {1: 'male', 2: 'female'}
education_dict ={    1: '<9th grade', 2: '9-11th grade', 3: 'HS or GED', 4: 'Some college / AA', 5: 'College or above', 7: 'Other', 8: 'Other'}

df_sans_outliers["smoking"] = df_sans_outliers["smoking"].replace(smoking_dict)
df_sans_outliers["gender"] = df_sans_outliers["gender"].replace(gender_dict)
df_sans_outliers["education"] = df_sans_outliers["education"].replace(education_dict)

import matplotlib.pyplot as plt
# Utiliser Seaborn Pairplot pour visualiser les relations entre toutes les variables.
colonnes = ['smoking', 'gender', 'age', 'education', 'weight', 'height', 'bmi']
sns.pairplot(df_sans_outliers[colonnes])
plt.show()
# Créer des graphiques individuels pour observer la distribution ou la corrélation de chaque attribut.
for col in colonnes:
  sns.histplot(df_sans_outliers[col])
  plt.show()
# Sauvegarder le dataset nettoyé au format CSV (ou autre)
df_sans_outliers.to_csv(r"C:\\Users\\Rehmi Salma\\Desktop\\data_finale.csv", index=False)
# Les requêtes SQL Via SQLAlchemy
from sqlalchemy import create_engine, text ,select, func , MetaData, Table, case
from dotenv import load_dotenv
import os
# Insérer les données nettoyées dans une table PostgreSQL nommée, par exemple :
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
try:
    df_sans_outliers.to_sql("health_data_cleaned", engine, if_exists='replace', index=False)
    print("Données insérées avec succès dans PostgreSQL !")
except Exception as e:
    print("Erreur lors de l'insertion :", e)

metadata = MetaData()
table = Table("health_data_cleaned", metadata, autoload_with=engine)

# Quelle est la répartition des individus par genre (gender)
stmt = select(table.c.gender, func.count().label("nombre_individus")).group_by(table.c.gender).order_by(func.count().desc())
with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"{row.gender}: {row.nombre_individus}")

# Quelle est la répartition des individus en fonction de leurs habitudes de tabagisme (smoking) ?
stmt = select(table.c.smoking, func.count().label("nombre_individus")).group_by(table.c.smoking).order_by(func.count().desc())
with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"{row.smoking}: {row.nombre_individus}")

# Quelle est la moyenne de l'IMC (bmi) pour chaque genre (gender) ?
stmt = select(table.c.gender, func.avg(table.c.bmi).label("moyenne_bmi")).group_by(table.c.gender)
with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"{row.gender}: {row.moyenne_bmi:.2f}")

#Comment les individus se répartissent-ils en fonction de leur niveau d'éducation (education) ?

stmt = (
    select(table.c.education, func.count().label("nombre_individus"))
    .group_by(table.c.education)
    .order_by(func.count().desc())
)

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"{row.education}: {row.nombre_individus}")
#Quelle est l'évolution de l'IMC moyen (bmi) en fonction des tranches d'âge (par exemple, 18-30, 31-50, 51+) ?
age_group = case(
    (table.c.age.between(18, 30), '18-30'),
    (table.c.age.between(31, 50), '31-50'),
    (table.c.age >= 51, '51+'),
    else_='Unknown'
).label("age_group")

stmt = (
    select(age_group, func.avg(table.c.bmi).label("avg_bmi"))
    .group_by(age_group)
    .order_by(age_group)
)

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"Tranche d'âge: {row.age_group} - IMC moyen: {row.avg_bmi:.2f}")

# Quelle est la moyenne d'âge (age) pour chaque catégorie de tabagisme (smoking) ?
stmt = (
    select(table.c.smoking, func.avg(table.c.age).label("avg_age"))
    .group_by(table.c.smoking)
    .order_by(table.c.smoking)
)

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"Tabagisme: {row.smoking} - Âge moyen: {row.avg_age:.2f}")
# Quelle est la moyenne d'âge (age) pour chaque catégorie de tabagisme (smoking) ?
stmt = (
    select(
        table.c.smoking,
        func.avg(table.c.age).label("moyenne_age")
    )
    .group_by(table.c.smoking)
    .order_by(table.c.smoking)
)

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(f"Tabagisme: {row.smoking} - Moyenne d'âge: {row.moyenne_age:.2f}")