
# Heliocity: Backend Challenge

## Description of Various Functionalities

> ### 0a. The `main.py` file provides an overview of a possible scenario (see section C below) combining all described functionalities.

> ### 0b. The `tests.py` file allows running tests for different functionalities.

### 1. The `database_handler.py` file with the `DatabaseHandler` class and its `process_csv_file()` method

- Imports `.csv` files into the database:

    1. From the weather API
    2. From the calculator

> ### Import Optimization:
> Uses various parallelism methods (`map_async`, `apply_async`, `map`) from Python's native `multiprocessing.Pool` class.
>    
> Strategies under development:
> - Splitting into smaller files
> - Implementation in a low-level language like Rust

### 2. The `database_selector.py` file and its associated `DatabaseSelector` class with various methods

- Adjusts weather data from a 5-minute to a 15-minute time step to ajust to the calculator's step.
  
> Upcoming features:
> - Dynamic specification of initial and target time steps
> - Creates SQL sub-tables containing the selected data range (time range, temperature, etc.) generated from the original table.

### 3. The `json_generator.py` file and its associated `JSONGenerator` class

- Manipulates database data to generate a `.json` file for visualization.
- Provides data preview with the option to filter out aberrant values.

# Getting Started

## Create a Virtual Environment

### A. Prerequisites

> Guidelines for a Linux environment

- Configured and running PostgreSQL server.
- Creation and configuration of a new database.
- Edit the `config.json` file with necessary parameters for connecting to the database.

### B. Installation

#### Clone files from the Git repo:

```bash
git clone https://github.com/DevprojectEkla/HelioCity
cd HelioCity
```
### Create a Virtual Environment:

```bash
python -m venv env
```

### Activate the Virtual Environment:

```bash
source env/bin/activate  # On Linux
```

### Install Dependencies:

```bash
pip install -r requirements.txt
```

### (Optional) Create a `data/` Folder for Your `.csv` Files:

```bash
mkdir data
```

## C. Usage Scenario Example Using Our Classes

The `main.py` file can be launched with arguments; otherwise, a series of prompts will ask for:

- The table name (either an existing table name or the name for a new table to be created in the database from the imported file).
- If applicable, the name of the `.csv` file to import into the database.
- Optionally, use the `-f` flag to specify a simple import method; absence of the flag defaults to a parallelism-based import.

```bash
python main.py [table_name] [path_to_csv_file] [-f]
```

### Imagined Scenario Type:

- Import a table from `./data/meteo_data.csv` in preprocessing or `./data/test_helio.csv` in post-processing.
- Filter out aberrant data and specify a time interval.
- Insert a new variable called `python_calc`* into a table for time-based representation.
> &#42; In this scenario, it involves preprocessing wind chill temperature as a function of temperature, wind speed, and relative humidity. In post-processing, it's a test calculation (to be adjusted with a relevant formula).
- Generate a `.json` file from this preview data for future use in another context.

## D. Independent Usage of Different Scripts

### Importing CSV Data into PostgreSQL

#### Run `database_handler.py`:

```bash
python database_handler.py
```

You will be prompted for:

- The name of the new table to create (default: `meteo_data`).
- The path to the `.csv` data file (default: `./data/meteo_data.csv`).
- Specify data origin (weather or calculator); calculator column processing takes place in adjustable portions of the number of lines answered `'y'` if it's a large file. 'n' or '' in the case of a large file.

> Warning: Importing large `.csv` files from the calculator can take some time depending on the computer's memory capabilities. Adjust the value of the number of lines per portion to available memory.

### Post and Pre-Processing Data Manipulations

#### Using `DatabaseSelector` Class from `database_selector.py`:

Data manipulations can be performed using the `DatabaseSelector` class to create new tables in the database. It allows:

- Creating sub-tables by interval of interest.
- Aggregating weather data at the calculator's timestep.
- Inserting calculated variables from existing table variables.

For a test, simply run the command:

```bash
python database_selector.py
```

Follow the instructions...

#### Using `JSONGenerator` Class from `json_generator.py`:

This class only reads from the database and does not write to it. It facilitates easy manipulation of data in dataframes for visualization and is used to generate a `.json` format.
