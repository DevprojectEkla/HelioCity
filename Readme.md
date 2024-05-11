
# Heliocity : défi backend

## Fonctionnalités

### 1. Le fichier import_csv_to_pgsql.py  

- sert à importer dans une base de données postgresql les données d'un fichiers .csv :

    1. de l'API météo en vue du prétraitement 

    2. du calculateur en vue du posttraitement 
  
> ### Optimisation de l'importation:
> stratégies en cours d'élaboration:  
> - Multiprocessing: l'utilisation de la classe Pool() de la librairie multiprocessing pourrait permettre d'accélérer considérablement l'importation d'un fichier de calculateur.  
> - division en fichiers plus petits
> - implémentation en un langage bas niveau comme Rust

### 2. Le fichier aggregate_step_for_helio.py 

- permet de ramener un fichier météo d'un pas de temps de 5 min au pas de temps de 15 min du calculateur 
> à venir: spécification dynamique du pas de temps initial et du pas de temps d'arrivée

### 3. Le fichier select_interval.py 

- permet la création d'un sous tableau sql contenant la sélection demandée (plage de temps; température,...) généré à partir du tableau original.

### 4. Le fichier plot_some_data.py

- permet une première visualisation rapide des données importées avec filtrage des valeurs abérrantes.

## A. pré-requis:  

> indication pour un environnement Linux

- un server postgresql configuré et démarré 

- la création et la configuration d'une nouvelle base de donnée

- l'édition du fichier config.json avec les paramètres nécessaires en vue de la connexion à la base de donnée.

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

## C. Importation des données csv vers postgresql 

### Lancer le fichier import_csv_to_pgsql.py:

`python import_csv_to_pgsql.py`

Une série de trois prompts vous demandera:

- le nom du nouveau tableau à créer
ex: `meteo_data` (c'est la valeur par défaut)

- le chemin d'accès vers le fichier de données au format .csv
ex `./data/meteo_data.csv` (c'est la valeur par défaut)

- une spécification de l'origine des données (météo ou calculateur; le traitement des colonnes est différent dans les deux cas) répondre `'y'` s'il s'agit d'un fichier de données météo. Tout autre réponse traite le cas d'un fichier de calculateur.

> Avertissement: L'importation des fichiers .csv volumineux provenant du calculateur a lieu par portion de 200000 lignes (valeur ajustable) et peut prendre un temps significatif comparé à l'extraction des données météo qui est quasi instantanée.
~~> Avec l'utilisation du multiprocessing les portions sont désormais de 500000 lignes (valeur réglables dynamiquement) pour un temps d'exécution divisé par 10~~

## D. Prétraitement

### Ajuster le pas des données d'entrées  
lancer simplement la commande:  

`python aggregate_step_for_helio.py`  
> **Attention:** Il s'agit d'un simple filtrage des données météo afin de ne garder que les données à intervalle de 15 minutes. Le filtrage est hardcodé à cette valeur mais on pourrait le rendre dynamique. 

suivre les instructions...

## E. Manipulation des données Post et Pré traitement:
 
 ### Sélectionner une plage de valeur pour une champ donné (ex: champ: Date, start: 2023-01-01 end: 2023-01-31)


 > **Attention:** Il faut d'abord avoir importé les données .csv dans la base de données avant d'exécuter ce script.  
 Dans le cas de la table du calculateur, très volumineuse, la commande sql n'est pas optimisée. Il faudrait probablement d'abord la découper en tableaux plus petits. Ceci est encore en chantier...  

`python select_interval.py`

suivre les instructions...
