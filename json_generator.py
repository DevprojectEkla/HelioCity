import json, threading
from multiprocessing import Process
import numpy as np
import pandas as pd
import matplotlib.pyplot as plot
from datetime import datetime
from connect_db import conn_alchemy_with_url
from database_handler import DatabaseHandler



class JSONGenerator(DatabaseHandler):
    """This is our main Class to generate Json file for visualization
    JSONGenerator must connect to the DB so it inherits from DatabaseHandler.
    JSONGenerator can be used to manipulate data from database; it uses pandas 
    DataFrame objects and can even help to previsualize data before generating
    the JSON file with its .plot_data() method.
    The Plot process uses matplotlib wich blocks and cannot be threaded safely.
    Implementation of multiprocessing for this is not trivial and we are working
    on it.
    """
    
    def __init__(self):
        # this will be populated by the init_generator method
        super().__init__()
        self.json_format = {}
        self.json_filename = ''
        self.timestamp = ''
        self.query = ''
        self.table_name = ''
        self.columns = []
        self.dates_column = None 
        self.flag_ts = False
        self.df = None 
        self.graph_type = ''
        self.x_axis = ''
        self.y_axis = ''
        self.z_axis = ''
        self.x_labels = []
        self.y_labels = []
        self.pivot = None
        self.plot_process = None
    
    def disconnect(self):
        # You should call this when you are done with the class Instance
        super().disconnect()
        # if we use multiprocessing we would need this to kill any remaining
        # spawned process
        if (hasattr(self,'plot_process') 
        and isinstance(self.plot_process,Process) 
        and self.plot_process.is_alive()):
            self.plot_process.terminate()
    
    def init_generator(self, table_name, columns, graph_type):
        # Don't forget to use .connect() method inherited from DatabaseHandler
        # before using this method. This one is meant to be called right after
        # the database connection to populate our class attributes
        # You will have to specify the table name of the table you want to
        # interact with, the columns you want to manipulate and the graph_type
        # (heat, scatter,...)
        self.timestamp = datetime.now().strftime('%d-%m-%y')
        self.json_filename = f"./data/{self.timestamp}_{self.x_axis}-{self.y_axis}.json"
        self.columns = columns
        if len(self.columns) >= 2:
            self.x_axis, self.y_axis = self.columns[:2]
            if len(self.columns) == 3:
                self.z_axis = self.columns[2]
        else:
            raise ValueError(
                    "init failed self.columns must have 2 or 3 elements")
        self.dates_column = next((col for col in ['date','Date','ts'] 
                                  if col in self.columns),None)
        self.graph_type = graph_type
        self.table_name = table_name
        self._build_query()
        self._init_dataframe()

    def _build_query(self):
        # this is a private method. You should'not need to call it directly
        # it simply grabs the columns you specified in init_generator to build
        # the string for the SQL query
        select_columns = '"'+'", "'.join(self.columns)+'"'
        self.query = f'SELECT {select_columns} FROM "{self.table_name}"'

    def _init_dataframe(self):
        if self.dates_column is not None:
            self.df = pd.read_sql(self.query,
                                  self.sql_engine,
                                  parse_dates=[self.dates_column])
        else:
            self.df = pd.read_sql(self.query,self.sql_engine)

    def _split_timestamps(self):
        if self.dates_column is not None:
            self.df[self.dates_column] = pd.to_datetime(
                                            self.df[self.dates_column])
            self.df['day'] = self.df[self.dates_column].dt.date
            self.df['time'] = self.df[self.dates_column].dt.time
            self.df['hours'] = self.df[self.dates_column].dt.hour
            
    def display_df(self):
        if self.df is not None:
            print(self.df,[self.df[col] for col in self.columns])
        else:
            print(":: Warning :: DataFrame is None")

    def filter_df(self,axis, min, max):
        try:
            mask = (self.df[axis] >= min) & (self.df[axis] <= max)
            self.df = self.df[mask]
        except Exception as e:
            print(f"cannot filter dataframe axis: {axis} with values min:{min} "
                  f"and max:{max}\nerror:",e)

    def order_df_by(self,column_name):
        try:
            self.df= self.df.sort_values(by=column_name)
        except Exception as e:
            print(f"Unable to sort the dataframe for column: {column_name} in "
                  f"table: {self.table_name}")

    def _make_pivot(self):
        try:
    
            self.pivot = self.df.pivot_table(index=self.y_axis,
                                           columns=self.x_axis,
                                           values=self.z_axis,
                                           aggfunc='mean')
        except Exception as e:
            print("Error creating pivot",e)
        
    def resample_times_to_hours(self):
        #TODO
            hourly_times = self.df.set_index('times').resample('H').sum().index

    def _create_time_labels(self):
        try:
            dates = pd.to_datetime(self.pivot.columns)

    # Filter times to display only hourly labels
            hourly_times = self.df['hours']            
            unique_times = np.unique(self.df['hours'].values)
            times_labels = [time if i == 0 or time != unique_times[i-1] 
                            else '' for i, time in enumerate(unique_times)] 

            # when there is too much data we don't want to overload the graph
            # with labels on each ticks so we divide the number of labels while
            # keeping data to be represented
            # since there is as much 'days' value in the table 
            # as there is 'time' we only need to take the common length n
            dates = dates.strftime('%d-%m-%Y')
            self.x_labels = [date for date in dates] 
            # unique_times = np.unique(self.df['time'].values)
            self.y_labels = times_labels
            # self.y_labels = [time for time in hourly_times]
            # self.y_labels = [time if i == 0 or time != unique_times[i-1]
            #                  else '' for i, time in enumerate(unique_times)] 
        except Exception as e:
            print("failed to create 'day' and 'time' labels\nerror:",e)
            print("pivot parameter 'columns' are not a timestamp format\nerror:",e)
    def _rearrange_columns(self):
            # to plot a heatmap with days in abscissa and time in ordinate
            # with plots of the quantity we want to represent in color
            # the z axis is set to the value of y_axis (e.g temperature)
            # and the x_axis 'date' is now split in:
            # x_axis ='day' y_axis = 'time'
            self.z_axis = self.y_axis
            self.x_axis = 'day'
            self.y_axis = 'time'

    def plot_data(self):
        if self.graph_type == 'heat':
            try:
                self._split_timestamps()
                self._rearrange_columns()
                self._make_pivot()
                self._create_time_labels()
                self.plot_heatmap()
            except Exception as e:
                print("Unable to create labels for our axis due to:",e)
        else:
           self.plot_scatter() 

    def plot_scatter(self):
        self.df.plot(x=self.x_axis,
                         y=self.y_axis,
                         kind='line',
                         marker='o',
                         linestyle='-')
        plot.xlabel(self.x_axis)
        plot.ylabel(self.y_axis)
        plot.grid(True)

        plot.show()

    def plot_heatmap(self):
        try:
            plot.figure(figsize=(10, 6))
        
            plot.imshow(self.pivot, cmap='cool',aspect='auto',
                    interpolation='nearest')
            plot.colorbar(label=self.z_axis)
            plot.xticks(range(len(self.x_labels)), self.x_labels, rotation=0)
            plot.yticks(range(len(self.y_labels)), self.y_labels, rotation=0)
            plot.xlabel(self.x_axis)
            plot.ylabel(self.y_axis)
            plot.show()
        except Exception as e:
            print("Unable to plot the heatmap due to:\nerror:",e)
        # self.process = Process(target=plot.show)
        # self.process.start()
            # self.plot_thread = threading.Thread(target=plot.show)
            # self.plot_thread.start()
      
    def format_json(self):
        # Creates the json data structure of key value pairs 
        # based on column_names and values of self.columns
        data = {}
        for key,column in zip(["x","y","z"],self.columns):
            data[key] = self.df[column].tolist()
        data["type"] = self.graph_type
        self.json_format = json.dumps(data,
                                      default=lambda x: str(x)
                                      if isinstance(x,datetime)
                                      else x,
                                      indent=2
                                      )

    def write_data_into_json(self):
        try:
            self.format_json()
            with open(self.json_filename, 'w') as json_file:

                json.dump(self.json_format, json_file)

            print(f"Data saved to {self.json_filename}")
        except Exception as e:
            print(f"Error: could not write the json file => {e}")


if __name__ == '__main__':
    # Test our class

    json_gen = JSONGenerator()
    json_gen.connect()
    json_gen.init_generator('helio_data',['ts','tpv_sim'],'linear')
    # json_gen.filter_df('Temperature',0,401)
    json_gen.display_df()
    input("continue?")
    json_gen.plot_data()
    input("continue?")
    json_gen.write_data_into_json()
    json_gen.disconnect()
