from function import Nettoyage_csv
from connexion import Connexion
import traitement_all_weeks_countries 
import pandas as pd
import os

# PATH = "NetflixTop10_app/"

#
# !!! Changer les informations de connexion dans le fichier connexion.py
# !!! avoir créé une base de données, et rentrer cette information dans le fichier connexion.py

# Exécuter ce fichier pour 
# - créer les tables
# - récupérer les fichiers csv
# - les traiter avec la classe Nettoyage_csv
# - insérer les données en bdd
#



# les fichiers csv à importer
fichiers_csv=['all-weeks-countries.csv','all-weeks-global.csv','most-popular.csv', "all-countries.csv", "all-shows.csv"]


# régler les problèmes de chemins
dossier = os.getcwd()
path1 = os.path.join(dossier,fichiers_csv[0])
path2 = os.path.join(dossier,fichiers_csv[1])
path3 = os.path.join(dossier,fichiers_csv[2])
path4 = os.path.join(dossier,fichiers_csv[3])
path5 = os.path.join(dossier,fichiers_csv[4])



# effectuer le pré-traitement du fichier all-weeks-countries.csv
df = pd.read_csv('all-weeks-countries.csv')
cl = traitement_all_weeks_countries.Traitement(df)
df = cl.remplacement_nom_par_id('country_name')
df = cl.remplacement_nom_par_id('show_title')
df = df.drop('country_iso2', axis=1)
df = df.drop('season_title', axis=1)
df = df.drop('category', axis=1)
df.to_csv('all-weeks-traited.csv', index=False)
path1 = os.path.join(dossier,'all-weeks-traited.csv')





# AllWeeksCountries
sql = """CREATE TABLE IF NOT EXISTS `allWeeksCountries` (
`id_allWeeksCountries` INTEGER PRIMARY KEY AUTO_INCREMENT,
`id_country` INT NULL,
`week` VARCHAR(255) NULL,
`weekly_rank` INT NULL,
`id_show` INT NULL,
`cumulative_weeks_in_top_10` INT  NULL
);"""

Connexion().req(sql,commit = True)



sql = "INSERT INTO allWeeksCountries(id_country, week,weekly_rank,id_show,cumulative_weeks_in_top_10)VALUES(%s,%s,%s,%s,%s);"
Nettoyage_csv(path1).insertion_donnees(sql)



# AllWeeksGlobal
sql = """CREATE TABLE IF NOT EXISTS allWeeksGlobal(
  id_all_weeks_global        INT PRIMARY KEY AUTO_INCREMENT,
  week                       DATE  NOT NULL,
  category                   VARCHAR(255) NOT NULL,
  weekly_rank                INTEGER  NOT NULL,
  show_title                 VARCHAR(255) NOT NULL,
  season_title               VARCHAR(255),
  weekly_hours_viewed        INTEGER  NOT NULL,
  cumulative_weeks_in_top_10 INTEGER  NOT NULL
);"""

# Connexion().req(sql,commit = True)



# sql = "INSERT INTO allWeeksGlobal(week, category, weekly_rank, show_title, season_title, weekly_hours_viewed, cumulative_weeks_in_top_10)VALUES(%s,%s,%s,%s,%s,%s,%s);"
# Nettoyage_csv(path2).insertion_donnees(sql)



# MostPopular
sql = """CREATE TABLE IF NOT EXISTS mostPopular(
  `id_most_popular`            INT PRIMARY KEY AUTO_INCREMENT,
  `category`                   VARCHAR(255) NOT NULL,
  `rank`                       INT,
  `show_title`                 VARCHAR(255) NOT NULL,
  `season_title`               VARCHAR(255),
  `hours_viewed_first_28_days` INT
);"""

# Connexion().req(sql,commit = True)

# sql = "INSERT INTO mostPopular(`category`, `rank`, `show_title`, season_title, hours_viewed_first_28_days)VALUES(%s,%s,%s,%s,%s);"
# Nettoyage_csv(path3).insertion_donnees(sql)



# Countries
sql = """CREATE TABLE IF NOT EXISTS countries(
  `id_country`        INT PRIMARY KEY AUTO_INCREMENT,
  `country_name`      VARCHAR(255) NULL,
  `country_iso2`      VARCHAR(255) NULL
);"""
Connexion().req(sql,commit = True)



sql="INSERT INTO countries(`country_name`, `country_iso2`)VALUES(%s,%s);"
Nettoyage_csv(path4).insertion_donnees(sql)


# Shows
sql = """CREATE TABLE IF NOT EXISTS shows(
  `id_show`                    INT PRIMARY KEY AUTO_INCREMENT,
  `category`                   VARCHAR(255) NOT NULL,
  `show_title`                 VARCHAR(255) NOT NULL,
  `season_title`               VARCHAR(255)
);"""
Connexion().req(sql,commit = True)

sql="INSERT INTO shows(`category`, `show_title`, `season_title`)VALUES(%s,%s,%s);"
Nettoyage_csv(path5).insertion_donnees(sql)



# ajouter les clés étrangères
sql="""ALTER TABLE `allWeeksCountries` ADD FOREIGN KEY (`id_country`) REFERENCES `countries`(`id_country`) ON DELETE RESTRICT ON UPDATE RESTRICT;"""
Connexion().req(sql,commit = True)

sql= """ALTER TABLE `allWeeksCountries` ADD FOREIGN KEY (`id_show`) REFERENCES `shows`(`id_show`) ON DELETE RESTRICT ON UPDATE RESTRICT;"""
Connexion().req(sql,commit = True)

# create view
sql = """CREATE VIEW films_france AS
SELECT shows.show_title FROM shows JOIN allWeeksCountries ON shows.id_show=allWeeksCountries.id_show JOIN countries ON countries.id_country=allWeeksCountries.id_country
WHERE countries.country_name = 'France';"""
Connexion().req(sql,commit = True)