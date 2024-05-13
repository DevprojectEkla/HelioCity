import inspect, traceback
from database_handler import DatabaseHandler
from database_selector import DatabaseSelector
""" This file can be used to run tests of main functionnalities, main methods
of DatabaseHandler and DatabaseSelector. Each function is named from the name 
of the method to test prefixed by test_* """

def test_aggregate_to_helio_step():
    try:
        manager = DatabaseSelector()
        manager.connect()
        manager.init_import('meteo_data','','')
        manager.aggregate_values_to_helio_step(subtable_name='test_aggregate_to_step')
        manager.disconnect()
        return 0
    except Exception as e:
        raise RuntimeError(f"Error occurred during {inspect.currentframe().f_code.co_name}") from e

    
def test_insert_variable():
    try:
        manager = DatabaseSelector()
        manager.connect()
        # test for inserting new variables to the table
        manager.init_import('meteo_data','','')
        manager.insert_variables_from_python_formula(['wind_speed','rel_humidity'])
        manager.disconnect()
        return 0
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error occurred during {inspect.currentframe().f_code.co_name}") from e


def test_select_interval():
# test for the creation of subtables with selected values 
    try:
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
        manager.disconnect()
        return 0
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error occurred during {inspect.currentframe().f_code.co_name}") from e


def test_import_db_meteo():
    try:
        db_handler = DatabaseHandler()
        db_handler.connect()
        db_handler.init_import('test_meteo_import_db', './data/meteo_data.csv', True)
        db_handler.process_csv_file()
        db_handler.disconnect()
        return 0
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error occurred during {inspect.currentframe().f_code.co_name}") from e

def test_import_db_helio_results():
    try:
        db_handler = DatabaseHandler()
        db_handler.init_import('test_helio_data_import', './data/test_helio.csv', False)
        db_handler.connect()
        db_handler.process_csv_file()
        db_handler.disconnect()
        return 0
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error occurred during {inspect.currentframe().f_code.co_name}") from e


def tests():
    functions_to_test = [test_insert_variable,test_select_interval,test_import_db_meteo,
                         test_import_db_helio_results,test_aggregate_to_helio_step]
    for i,func in enumerate(functions_to_test):
        try:
            
            print(f"{len(func.__name__)*'='}===============")
            print(f"TEST {i} {func.__name__} running")
            print(f"{len(func.__name__)*'='}===============")

            result = func()
            print(f"{len(func.__name__)*'o'}oooooooooooooooooooooooooooooooooooo")
            print(f"{func.__name__} executed successfully. exit code:", result)
            print(f"{len(func.__name__)*'o'}oooooooooooooooooooooooooooooooooooo")
        except Exception as e:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(f"Error occurred in Test {i} function:", func.__name__)
            print("Error message:", e)
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")


    
if __name__ == '__main__':

        tests()

