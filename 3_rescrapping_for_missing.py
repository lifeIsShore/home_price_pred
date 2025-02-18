import pandas as pd

# Paths to the CSV files
filtered_dataset_path = r'C:\Users\ahmty\Desktop\projects\price-pred\dataframe\filtered_dataset.csv'
merged_data_path = r'C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged\merged_data_BEFORE_control.csv'
output_path = r'C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe\missing_zip_codes.csv'

# Read the filtered dataset CSV
filtered_df = pd.read_csv(filtered_dataset_path, sep=';')

# Read the merged data CSV
merged_df = pd.read_csv(merged_data_path)

# Extract zip codes and cities from both DataFrames
filtered_data = filtered_df[['Name', 'PLZ Name (short)']]  # Extract both Zip Code and City
filtered_data['Name'] = filtered_data['Name'].astype(str)  # Ensure 'Name' is in string format
merged_zip_codes = set(merged_df['Zip Code'].astype(str))  # Zip codes from merged dataset

# Find missing zip codes (zip codes in filtered_df but not in merged_df)
missing_data = filtered_data[~filtered_data['Name'].isin(merged_zip_codes)]

# Save the missing data (ZIP code and City) to a CSV file
missing_data.to_csv(output_path, index=False)

print(f"Missing zip codes and cities saved to {output_path}")
