from database_handler import DatabaseHandler
from database_selector import DatabaseSelector
from utils import input_source


def test_aggregate_step_to_helio():
    manager = DatabaseSelector()
    manager.connect()
    manager.init_import('meteo_data','','')
    manager.aggregate_values_to_helio_step()
    return 0

    
def test_insert_variable():
    manager = DatabaseSelector()
    manager.connect()
    # test for inserting new variables to the table
    manager.init_import('meteo_data','','')
    manager._create_new_column('python_calc')
    manager.insert_variables_from_python_formula(['wind_speed','rel_humidity'])
    return 0

def test_select_interval():
# test for the creation of subtables with selected values 
    manager = DatabaseSelector()
    manager.connect()
    available_tables = manager.get_available_tables()
    original_table_name = 'meteo_data' 
    new_table_name = 'test_select_interval' 
    fields = manager.get_fields_in_table('meteo_data')
    column_name = 'Date' 
    start = '2023-12-01' 
    end = '2024-06-30' 
    manager.select_interval(start, end, column_name, original_table_name, new_table_name)
    return 0

def test_import_db_meteo():
    db_handler = DatabaseHandler()
    db_handler.connect()
    db_handler.init_import('test_import_db', './data/meteo_data.csv', 'y')
    db_handler.process_csv_file()
    db_handler.disconnect()
    return 0

def test_import_db_helio_results():
    db_handler = DatabaseHandler()
    db_handler.connect()
    db_handler.init_import('test_import_db', './data/test_helio.csv', 'n')
    db_handler.process_csv_file()
    db_handler.disconnect()
    return 0

    


def main():
    functions_to_test = [test_insert_variable,test_select_interval,test_import_db_meteo,
                         test_import_db_helio_results,test_aggregate_step_to_helio]
    for i,func in enumerate(functions_to_test):
        try:
            print("===================================")
            print(f"TEST {i} {func.__name__} running")
            print("===================================")

            result = func()
            print("===================================================")
            print(f"{func.__name__} executed successfully. exit code:", result)
            print("===================================================")
        except Exception as e:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(f"Error occurred in Test {i} function:", func.__name__)
            print("Error message:", e)
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")


    
if __name__ == '__main__':

        main()

