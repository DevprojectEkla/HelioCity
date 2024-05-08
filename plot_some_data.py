from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plot
from connect_db import connect_with_alchemy, open_config

dbname, user, password, host, port = open_config()
engine = connect_with_alchemy(dbname, user, password, host, port)
table_name = 'selected_date'
x_axis = input("select data to plot for the x-axis:\n").strip() or 'Date'
y_axis = input("select data to plot for the y-axis:\n").strip() or 'Solar_radiation'
query = f'SELECT "{x_axis}", "{y_axis}" FROM "{table_name}"'

# Specify some threshold to generate a filter 
threshold_max = 400
threshold_min = 50



df = pd.read_sql(query, engine, parse_dates=[x_axis])

# Filter the temperature data from any outliers
mask = (df[y_axis] >= threshold_min) & (df[y_axis] <= threshold_max)
filtered_df = df[mask]

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

