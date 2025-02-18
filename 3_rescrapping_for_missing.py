import pandas as pd

# Paths to the CSV files
filtered_dataset_path = r'C:\Users\ahmty\Desktop\projects\price-pred\dataframe\filtered_dataset.csv'
merged_data_path = r'C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged\merged_data_BEFORE_control.csv'
output_path = r'C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe\missing_zip_codes.csv'

# Read the filtered dataset CSV
filtered_df = pd.read_csv(filtered_dataset_path, sep=';')

# Read the merged data CSV
merged_df = pd.read_csv(merged_data_path)

# Extract zip codes from both DataFrames
filtered_zip_codes = set(filtered_df['Name'].astype(str))  # Zip codes from filtered dataset
merged_zip_codes = set(merged_df['Zip Code'].astype(str))  # Zip codes from merged dataset

# Find missing zip codes (zip codes in filtered_df but not in merged_df)
missing_zip_codes = list(filtered_zip_codes - merged_zip_codes)

# Save the missing zip codes to a CSV file
missing_zip_codes_df = pd.DataFrame(missing_zip_codes, columns=['Missing Zip Codes'])
missing_zip_codes_df.to_csv(output_path, index=False)

print(f"Missing zip codes saved to {output_path}")
