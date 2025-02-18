import os
import pandas as pd

# Define the directory containing the CSV files
directory = r'C:\Users\ahmty\Desktop\projects\price-pred\scraped_data'
target= r'C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged'

# Initialize an empty list to store DataFrames
dataframes = []

# Loop through all files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        # Construct the full file path
        file_path = os.path.join(directory, filename)
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path, sep=";")
        
        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate all DataFrames into a single DataFrame
merged_df = pd.concat(dataframes, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv(os.path.join(target, 'merged_data.csv'), index=False)

print("All CSV files have been merged into 'merged_data.csv'")