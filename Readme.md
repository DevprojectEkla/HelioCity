
# Heliocity : défi backend

## Description des diverses fonctionnalités

> ### 0a. Le fichier main.py montre un aperçu d'un scénario possible (cf. C. ci-dessous) combinant toutes les fonctionnalités décrites plus bas.

> ### 0b. Le fichier tests.py permet de lancer les tests des différentes fonctionnalités

### 1. Le fichier database_handler.py avec la classe DatabaseHandler et sa méthode process_csv_file() 

- permet d'importer vers la base de données les fichiers .csv :

    1. de l'API météo  

    2. du calculateur  
  
> ### Optimisation de l'importation:
> Utilisation de diverses méthodes de parallélisme .map_async .apply_async .map de la class Pool de la librairie python native multiprocessing.
>    
> stratégies en cours d'élaboration:  
> - division en fichiers plus petits
> - implémentation en un langage bas niveau comme Rust

### 2. Le fichier database_selector.py et sa classe associée DatabaseSelector et ses différentes méthodes  

- permet de ramener un fichier météo d'un pas de temps de 5 min au pas de temps de 15 min du calculateur 
> à venir: spécification dynamique du pas de temps initial et du pas de temps d'arrivée

- permet la création de sous tableaux sql contenant la sélection demandée (plage de temps, température,...) généré à partir du tableau original.

### 3. Le fichier json_generator.py et sa classe associée

- permet la manipulation des données de la DB en vue de la génération d'un fichier .json pour la visualisation
- permet aussi une prévisualisation des données avec possibilité de filtrage des valeurs abérrantes.

# Getting Started

## A. pré-requis:  

> indication pour un environnement Linux

- un server postgresql configuré et démarré 

- la création et la configuration d'une nouvelle base de données

- l'édition du fichier config.json avec les paramètres nécessaires en vue de la connexion à la base de données.

## B. installation:

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

## C.  Exemple de Scénario d'utilisation de nos Classes  

Le fichier main.py peut être lancé avec des arguments, si ce n'est pas le cas une série de prompt demandera:  
    - le nom du tableau (selon le scénario il s'agit d'un nom de tableau existant ou du nom de tableau à créer dans la base de données à partir du fichier à importer)  
    - si c'est le cas, le nom du fichier .csv que l'on désire importer dans la BD  
    - éventuellement un flag -f permet de préciser une méthode d'importation simple, l'absence de flag vaut pour une importation utilisant le parallèlisme  

`python main.py [nom_du_tableau] [path_to_csv_file] [-f]`  

### type de scénario imaginé:  

- un tableau doit être importé à partir du fichier data/meteo_data.csv,  
- certaines données abbérantes doivent être filtrées et/ou un intervalle de temps doit être spécifié,  
- une nouvelle variable appelée python_calc* doit être insérée dans un tableau pour être représentée en fonction du temps  
  > &#42; il s'agit dans le scénario du calcul de la température ressentie qui est une fonction de la température, de la vitesse du vent et de l'humidité relative.  
- un fichier .json doit ensuite être généré à partir de ces données de prévisualisation en vue d'une utilisation future dans un autre contexte.  

## D. Utilisation indépendante des différents scripts

### Importation des données csv vers postgresql

#### Lancer le fichier database_handler.py:

`python database_handler.py`

Une série de trois prompts vous demandera:

- le nom du nouveau tableau à créer  
ex: `meteo_data` (c'est la valeur par défaut)  

- le chemin d'accès vers le fichier de données au format .csv  
ex `./data/meteo_data.csv` (c'est la valeur par défaut)  

- une spécification de l'origine des données (météo ou calculateur); le traitement des colonnes du calculateur a lieu par portion ajustable de nombre de lignes) répondre `'y'` s'il s'agit d'un fichier de données peut volumineux et 'n' ou '' dans le cas d'un fichier volumineux.   
> Avertissement: L'importation des fichiers .csv volumineux provenant du calculateur peut prendre un certain temps selon les capacités mémoire de l'ordinateur. Il est nécessaire d'ajuster la valeur du nombre de lignes par portion à la mémoire disponible. 

### Manipulations de données Post et Pré traitement:

#### La classe DatabaseSelector:  

Les manipulations de données peuvent être effectuée via la classe DatabaseSelector pour générer de nouveau tableau dans la base de données.  
Elle permet:  
    - la création de sous-taleaux par intervalle d'intérêt  
    - l'aggrégation des données météo au pas du calculateur  
    - l'insertion de variables calculées à partir des variables d'un tableau existant  


pour un test lancer simplement la commande:  

`python database_selector.py`

suivre les instructions...

#### La classe JSONGenerator:

Cette classe n'accède qu'en lecture à la base de donnée elle n'y écrit rien. Elle permet de manipuler facilement les données dans des dataframe
pour une prévisualisation en graph des données à importer en format .json

pour un exemple lancer:

`python json_generator.py`


