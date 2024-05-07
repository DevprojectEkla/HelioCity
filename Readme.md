# Heliocity : défi backend


1) Le fichier import_csv_to_pgsql.py sert à extraire les données;

a. de l'API météo en vue du prétraitement   
b. du calculateur en vue du posttraitement 
  

2) Le fichier aggregate_step_for_helio.py permet de ramener un fichier météo d'un pas de temps de 5 min au pas de temps de 15 min du calculateur (à venir: spécification dynamique du pas de temps initiale et du pas de temps d'arrivée)

3) Le fichier select_interval.py permet la création d'un sous tableau sql contenant la sélection demandée (plage de temps) généré à partir du tableau original.

## A) pré-requis:  

=> indication pour un environnement Linux

a. un server postgresql configuré et démarré 

b. la création et la configuration d'une nouvelle base de donnée

c. l'édition du fichier config.json avec les paramètres nécessaires en vue de la connexion à la base de donnée.

## B) installation:

### Cloner les fichiers depuis le repo git:

`git clone https://github.com/DevprojectEkla/HelioCity`  
`cd HelioCity`

### Créer un environnement virtuel:

`python -m venv env`

### Entrer dans l'environnement virtuel

`activate`

### Installation des dépendances:

`pip install -r requirements.txt`

### (optionnel) Créer un dossier data/ dans lequel vous mettrez vos fichies .csv

`mkdir data`

## C) Importation des données csv vers postgresql 

### Lancer le fichier import_csv_to_pgsql.py:

`python import_csv_to_pgsql.py`

Une série de trois prompts vous demandera:

a. le nom du nouveau tableau à créer
ex: `meteo_data` (c'est la valeur par défaut)

b. le chemin d'accès vers le fichier de données au format .csv
ex `./data/meteo_data.csv` (c'est la valeur par défaut)

c. une spécification de l'origine des données (météo ou calculateur; le traitement des colonnes est différent dans les deux cas) répondre `'y'` s'il s'agit d'un fichier de données météo. Tout autre réponse traite le cas d'un fichier de calculateur.

> **Avertissement:** L'importation des fichiers .csv volumineux provenant du calculateur a lieu par portion de 5*10^4 lignes et peut prendre un temps significatif comparé à l'extraction des données météo qui est quasi instantanée.

## D) Prétraitement

lancer simplement la commande:

`python aggregate_step_for_helio.py`

suivre les instructions...

`python select_interval.py`

suivre les instructions...
