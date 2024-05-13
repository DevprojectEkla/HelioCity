from datetime import datetime
import json
import pandas as pd
import matplotlib.pyplot as plot
from connect_db import conn_alchemy_with_url, connect_with_alchemy, open_config
from database_handler import DatabaseHandler



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
        self.df = {} 
        self.timestamp = ''
        self.json_filename = ''

    def init_generator(self):
        self.timestamp = datetime.now().strftime('%d-%m-%y')
        self.json_filename = f'./data/{self.timestamp}_{x_axis}-{y_axis}.json'

    
    def build_query(self):
        select_columns = '", "'.join(self.columns)
        self.query = f'SELECT {select_columns} FROM "{table_name}"'

    def select_data_in_database(self):
        if 'date' in self.columns:
            self.df = pd.read_sql(self.query,self.sql_engine,parse_dates=['date'])
        else:
            
            self.df = pd.read_sql(self.query,self.sql_engine)


    def filter_df(self,axis, min, max):

        mask = (self.df[axis] >= min) & (self.df[axis] <= max)
        self.df = self.df[mask]

    def format_json(self):
        for column in self.columns:
            self.json_format[column] = self.df[column].tolist()
        self.json_format["type"] = self.graph_type

    def write_data_into_json(self):
        try:
            with open(self.json_filename, 'w') as json_file:

                json.dump(self.json_format, json_file)

            print(f"Data saved to {self.json_filename}")
        except Exception as e:
            print(f"Error: could not write the json file => {e}")

    def plot_data(self):
        pass


engine = conn_alchemy_with_url() 
table_name = 'selected_date'
x_axis = input("select data to plot for the x-axis:\n").strip() or 'date'
y_axis = input("select data to plot for the y-axis:\n").strip() or 'temperature'
z_axis = input("select data to plot for the z-axis:\n").strip() or 'solar_radiation'
plot_type = input("select a type for the plot:\n").strip() or 'heat'

# query = f'SELECT "{x_axis}", "{y_axis}" FROM "{table_name}"'
query = f'SELECT "{x_axis}", "{y_axis}","{z_axis}" FROM "{table_name}"'

# Specify some threshold to generate a filter 
threshold_max = 401
threshold_min = 51



df = pd.read_sql(query, engine, parse_dates=[x_axis])
df['date'] = pd.to_datetime(df['date'])

# Filter the DataFrame to include only one date per day
# df_filtered = df.groupby(df['date'].dt.date).first()
# Filter the temperature data from any outliers
# mask = (df[z_axis] >= threshold_min) & (df[z_axis] <= threshold_max)
# df = df[mask]
# df = df.fillna(0)
# filtered_df[x_axis] = filtered_df[x_axis].astype(str)
# filtered_df.loc[:, x_axis] = filtered_df[x_axis].astype(str)

print(df,df[x_axis],df[y_axis],df[z_axis])

heatmap_data = df.pivot(index=y_axis, columns=x_axis, values=z_axis)

date_index = pd.to_datetime(heatmap_data.columns)

date_labels = date_index.strftime('%Y-%m-%d')
date_labels = [date for date in date_labels] 

y_values = heatmap_data.index
print(date_index,y_values)
n = len(y_values) // 10
y_labels = [str(value) for value in y_values[::n]]


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
plot.imshow(heatmap_data, cmap='hot',aspect='auto', interpolation='nearest')
plot.colorbar(label=z_axis)
plot.xlabel(x_axis)
plot.xticks(range(len(date_labels)), date_labels, rotation=0)
plot.yticks(range(len(y_labels)), y_labels, rotation=0)
plot.ylabel(y_axis)
plot.show()
