import pandas as pd
import os
import glob

# Define the source and destination folders
source_folder = r'C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe'
destination_folder = r'C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe\merged_rescrapping'
output_file = os.path.join(destination_folder, 'merged_rescrapping.csv')

# Ensure the destination folder exists
os.makedirs(destination_folder, exist_ok=True)

# Find all CSV files in the source folder
csv_files = glob.glob(os.path.join(source_folder, "*.csv"))

# Read and merge all CSVs into a single column
all_zip_codes = []
for file in csv_files:
    df = pd.read_csv(file, dtype=str)  # Read as string to avoid decimals
    column_name = df.columns[0]  # Get the first column (assuming it's the zip codes)
    all_zip_codes.extend(df[column_name].dropna().tolist())  # Collect all zip codes

# Create a DataFrame with a single column
merged_df = pd.DataFrame(all_zip_codes, columns=['Zip Code'])

# Save the merged file without decimal points
merged_df.to_csv(output_file, index=False)

print(f"Merged zip codes saved to {output_file}")