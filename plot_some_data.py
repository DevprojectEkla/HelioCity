from datetime import datetime
import json
import pandas as pd
import matplotlib.pyplot as plot
from connect_db import connect_with_alchemy, open_config

dbname, user, password, host, port = open_config()
engine = connect_with_alchemy(dbname, user, password, host, port)
table_name = 'selected_date'
x_axis = input("select data to plot for the x-axis:\n").strip() or 'Date'
y_axis = input("select data to plot for the y-axis:\n").strip() or 'Solar_radiation'
plot_type = input("select a type for the plot:\n").strip() or 'S'

query = f'SELECT "{x_axis}", "{y_axis}" FROM "{table_name}"'

# Specify some threshold to generate a filter 
threshold_max = 400
threshold_min = 50



df = pd.read_sql(query, engine, parse_dates=[x_axis])

# Filter the temperature data from any outliers
mask = (df[y_axis] >= threshold_min) & (df[y_axis] <= threshold_max)
filtered_df = df[mask]
filtered_df[x_axis] = filtered_df[x_axis].astype(str)
data_dict = {
    'x': filtered_df[x_axis].tolist(),
    'y': filtered_df[y_axis].tolist(),
    'type': plot_type,
}

# Save data to JSON file
timestamp = datetime.now().strftime('%d-%m-%y')
json_filename = f'./data/{timestamp}_{x_axis}-{y_axis}.json'
with open(json_filename, 'w') as json_file:
    json.dump(data_dict, json_file)

print(f"Data saved to {json_filename}")
# Plot the data
filtered_df.plot(x=x_axis,
        y=y_axis,
        kind='line',
        marker='o',
        linestyle='-'
        )

plot.xlabel('Date')
plot.ylabel('Solar Radiation over Time')
plot.grid(True)
# Generate a timestamp for the file to save:
timestamp = datetime.now().strftime('%d-%m-%y')
plot.savefig(f'./graph/{timestamp}_{x_axis}-{y_axis}.pdf')
plot.show()

