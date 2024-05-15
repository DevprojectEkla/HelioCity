"""Ce fichier serait l'entrée du programme si on voulait utiliser les différentes
fonctionnalités de nos scripts dans un contexte déterminée.
Exemple: 
    """
import sys

from database_selector import DatabaseSelector
from json_generator import JSONGenerator
from utils import input_source

def main(table_name, csv_file_path='', flag=False):
    manager = DatabaseSelector.new(table_name, csv_file_path, flag)
    if csv_file_path != '':
        manager.process_csv_file()
    if not table_name in manager.get_available_tables() and csv_file_path == '':
        print("Table Name error.")
        print("You need to choose an existing table name or to choose a path to a valid .csv file")
        manager.disconnect()
        exit(1)
    if 'meteo' in table_name or 'meteo' in csv_file_path:
        print(":: Running meteo file example ::")
        meteo_file_example(manager)
    else:
        print(":: Running helio file example ::")

        helio_file_example(manager)
    # manager.aggregate_values_to_helio_step()
     
def helio_file_example(manager):
    manager.init_queries()
    manager.make_query(['ts','ei','edc_sim','tpv_sim'])
    input('\ncontinue ?\n')
    manager.select_scope('mpp','mpp-25-4','selected_scope')
    manager.table_name = 'selected_scope'
    manager.order_table_by('pos','sorted')
    manager.table_name = 'sorted'
    filtered_and_sorted = manager.table_name
    manager.insert_variables_from_python_formula(['ei','edc_sim',
                                                  'tpv_sim'])
    manager.disconnect()

    json_generator = JSONGenerator()
    json_generator.connect()
    json_generator.table_name = filtered_and_sorted
    json_generator.init_generator(json_generator.table_name,['ts','python_calc'],'linear')
    json_generator.display_df()
    input("continue?")
    json_generator.plot_data()
    input("continue?")
    json_generator.write_data_into_json()
    json_generator.disconnect()

    

def meteo_file_example(manager):
    manager.init_queries()
    manager.make_query(['wind_speed','Date'])
    input('\ncontinue ?\n')
    #manager.aggregate_values_to_helio_step('adjusted_to_helio_step')
    #manager.table_name = 'adjusted_to_helio_step'
    manager.select_interval(start=0,end=400,
                            column_name='Temperature',
                            new_table_name='filtered')
    manager.select_interval(start=0,end=400,
                            column_name='wind_speed',
                            new_table_name='filtered')
    
    manager.table_name = 'filtered'
    manager.order_table_by('Date','sorted')
     
    manager.table_name = 'sorted'
    filtered_and_sorted = manager.table_name
    manager.insert_variables_from_python_formula(['Temperature','wind_speed',
                                                  'rel_humidity'])
    manager.disconnect()

    json_generator = JSONGenerator()
    json_generator.connect()
    json_generator.table_name = filtered_and_sorted
    json_generator.init_generator(json_generator.table_name,['Date','python_calc'],'linear')
    json_generator.display_df()
    input("continue?")
    json_generator.plot_data()
    input("continue?")
    json_generator.write_data_into_json()
    json_generator.disconnect()

if __name__ == '__main__':
    simple_mode = False 
    if len(sys.argv) < 2:
        print("Usage with arguments: python script.py <table_name> [csv_file_path] [flag]")
        table_name, csv_file_path, simple_mode = input_source()
    elif len(sys.argv) == 2:
        print("Usage with arguments: python script.py <table_name> [csv_file_path] [flag]")
        table_name = sys.argv[1] 
        csv_file_path = ''
        flag = False

    elif len(sys.argv) <= 4:
        table_name = sys.argv[1]
        csv_file_path = sys.argv[2]
        print(":: multiprocessing mode import ::\n"
              "(add -f for small files and simpler import mode)")
        if len(sys.argv) == 4:
            if sys.argv[3] == '-f':
                print(":: simple import mode activated ::")
                simple_mode = 'y' 
    else:
        print("wrong number of arguments")
        simple_mode = ''
        exit(1)
    flag = True if simple_mode == 'y' or simple_mode == True else False

    main(table_name, csv_file_path, flag)
