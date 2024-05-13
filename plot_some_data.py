from datetime import datetime
import json
import pandas as pd
import matplotlib.pyplot as plot
from connect_db import conn_alchemy_with_url, connect_with_alchemy, open_config
from database_handler import DatabaseHandler
import numpy as np



class JSONGenerator(DatabaseHandler):
    
    def __init__(self):
        super().__init__()
        self.json_format = {}
        self.query = ''
        self.table_name = ''
        self.x_axis = ''
        self.y_axis = ''
        self.z_axis = ''
        self.columns = []
        self.graph_type = ''
        self.df = None 
        self.timestamp = ''
        self.json_filename = ''
        self.x_labels = []
        self.y_labels = []
        self.flag_ts = False
        self.dates_column = None 
        self.pivot = None

    def init_generator(self, table_name, columns, graph_type):
        self.timestamp = datetime.now().strftime('%d-%m-%y')
        self.json_filename = f"""./data/{self.timestamp}
                            _{self.x_axis}-{self.y_axis}.json"""
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
        select_columns = '"'+'", "'.join(self.columns)+'"'
        self.query = f'SELECT {select_columns} FROM "{self.table_name}"'

    def _init_dataframe(self):
        if self.dates_column is not None:
            self.df = pd.read_sql(self.query,
                                  self.sql_engine,
                                  parse_dates=[self.dates_column])
        else:
            self.df = pd.read_sql(self.query,self.sql_engine)

    def split_timestamps(self):
        if self.dates_column is not None:
            self.df[self.dates_column] = pd.to_datetime(
                                            self.df[self.dates_column])
            self.df['day'] = self.df[self.dates_column].dt.date
            self.df['time'] =self.df[self.dates_column].dt.time

    def display_df(self):
        if self.df is not None:
            print(self.df,[self.df[col] for col in self.columns])
        else:
            print(":: Warning :: DataFrame is None")


    def filter_df(self,axis, min, max):

        mask = (self.df[axis] >= min) & (self.df[axis] <= max)
        self.df = self.df[mask]

    def format_json(self):
        # Creates the json data structure of key value pairs 
        # based on column_names and values of self.columns
        for column in self.columns:
            self.json_format[column] = self.df[column].tolist()
        self.json_format["type"] = self.graph_type

    def _init_graph(self):
        if self.graph_type == 'heat':
            self._make_pivot()
        else:
            self.df.plot(x=self.x_axis,
                         y=self.y_axis,
                         kind='line',
                         marker='o',
                         linestyle='-')
        try:
            self._create_labels()
        except Exception as e:
            print("Unable to create labels for our axis")

    def plot_data(self):
        self._init_graph()
        plot.figure(figsize=(10, 6))
        plot.imshow(self.pivot, cmap='hot_r',aspect='auto',
                    interpolation='nearest')
        plot.colorbar(label=z_axis)
        plot.xlabel(x_axis)
        plot.xticks(range(len(self.x_labels)), self.x_labels, rotation=0)
        plot.yticks(range(len(self.y_labels)), self.y_labels, rotation=0)
        plot.ylabel(y_axis)


    def _make_pivot(self):
    
            self.pivot = self.df.pivot_table(index=self.y_axis,
                                           columns=self.x_axis,
                                           values=self.z_axis,
                                           aggfunc='mean')
        
    def _create_labels(self):
        try:
            dates = pd.to_datetime(self.pivot.columns)

            self.x_labels = dates.strftime('%Y-%m-%d')
            self.x_labels = [date for date in date_labels] 
            self.y_labels = df['time']
            unique_times = np.unique(df['time'].values)
            self.y_labels = [time if i == 0 or time != unique_times[i-1]
                             else '' for i, time in enumerate(unique_times)] 

        except Exception as e:
            print("columns are not a timestamp format")

    def write_data_into_json(self):
        try:
            with open(self.json_filename, 'w') as json_file:

                json.dump(self.json_format, json_file)

            print(f"Data saved to {self.json_filename}")
        except Exception as e:
            print(f"Error: could not write the json file => {e}")


if __name__ == '__main__':

    json_gen = JSONGenerator()
    json_gen.connect()
    json_gen.init_generator('selected_date',['date','temperature'],'linear')
    json_gen.split_timestamps()
    json_gen.display_df()
    input("continue?")
    json_gen.plot_data()
    input("continue?")
    json_gen.write_data_into_json()
    engine = conn_alchemy_with_url() 
    table_name = 'selected_date'
    x_axis = input(
            "select data to plot for the x-axis:\n").strip() or 'date'
    y_axis = input(
            "select data to plot for the y-axis:\n").strip() or 'temperature'
    z_axis = input(
            "select data to plot for the z-axis:\n").strip() or 'solar_radiation'
    plot_type = input(
            "select a type for the plot:\n").strip() or 'heat'

# query = f'SELECT "{x_axis}", "{y_axis}" FROM "{table_name}"'
    query = f'SELECT "{x_axis}", "{y_axis}","{z_axis}" FROM "{table_name}"'

# Specify some threshold to generate a filter 
    threshold_max = 401
    threshold_min = 51



    df = pd.read_sql(query, engine, parse_dates=[x_axis])
    df['date'] = pd.to_datetime(df['date'])
    df['day'] = df['date'].dt.date
    df['time'] = df['date'].dt.time

# Filter the DataFrame to include only one date per day
# df_filtered = df.groupby(df['date'].dt.date).first()
# Filter the temperature data from any outliers
# mask = (df[z_axis] >= threshold_min) & (df[z_axis] <= threshold_max)
# df = df[mask]
# df = df.fillna(0)
# filtered_df[x_axis] = filtered_df[x_axis].astype(str)
# filtered_df.loc[:, x_axis] = filtered_df[x_axis].astype(str)

    print(df,df[x_axis],df[y_axis],df[z_axis])

    pivot_df = df.pivot_table(index='time',
                              columns='day',
                              values=z_axis,
                              aggfunc='mean')

    dates = pd.to_datetime(pivot_df.columns)

    date_labels = dates.strftime('%Y-%m-%d')
    date_labels = [date for date in date_labels] 

    times_labels = df['time']
    unique_times = np.unique(df['time'].values)
    times_labels = [time if i == 0 or time != unique_times[i-1] else '' for i, time in enumerate(unique_times)] 
    # y_values = pivot_df.index
    # print(dates,y_values)
    # n = len(y_values) // 10
    # y_labels = [str(value) for value in y_values[::n]]


# data_dict = {
#     'x': filtered_df[x_axis].tolist(),
#     'y': filtered_df[y_axis].tolist(),
#     'z': filtered_df[z_axis].tolist(),
#     'type': plot_type,
# }

# # Save data to JSON file
# timestamp = datetime.now().strftime('%d-%m-%y')
# json_filename = f'./data/{timestamp}_{x_axis}-{y_axis}.json'
# with open(json_filename, 'w') as json_file:
#     json.dump(data_dict, json_file)

# print(f"Data saved to {json_filename}")
# Plot the data
# filtered_df.plot(x=x_axis,
#         y=y_axis,
#         kind='line',
#         marker='o',
#         linestyle='-'
#         )

# plot.xlabel('Date')
# plot.ylabel('Solar Radiation over Time')
# plot.grid(True)
# # Generate a timestamp for the file to save:
# timestamp = datetime.now().strftime('%d-%m-%y')
# plot.savefig(f'./graph/{timestamp}_{x_axis}-{y_axis}.pdf')
# plot.show()
    plot.figure(figsize=(10, 6))
    plot.imshow(pivot_df, cmap='hot_r',aspect='auto', interpolation='nearest')
    plot.colorbar(label=z_axis)
    plot.xlabel(x_axis)
    plot.xticks(range(len(date_labels)), date_labels, rotation=0)
    plot.yticks(range(len(times_labels)), times_labels, rotation=0)
    plot.ylabel(y_axis)
    # plot.show()
    heatmap_data = pivot_df.reset_index().to_dict(orient='records')

# Convert data to JSON
    heatmap_json = json.dumps(heatmap_data, indent=4)
    print(heatmap_json)  # Print JSON data
