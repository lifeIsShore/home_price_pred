import pandas as pd

# Paths to the CSV files
merged_data_path = r'C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged\merged_data_BEFORE_control.csv'
output_path = r'C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe\wrong_max_min.csv'

# Read the merged data CSV
merged_df = pd.read_csv(merged_data_path)

# Define the incorrect numerical values
wrong_values = {1872.0, 5404.0, 1316.0, 4937.0}

# Identify rows where any of the numerical columns contain the wrong values
wrong_records_df = merged_df[merged_df.apply(lambda row: any(value in wrong_values for value in row if isinstance(value, (int, float))), axis=1)]

# Extract unique zip codes from the wrong records
wrong_zip_codes = wrong_records_df['Zip Code'].astype(str).unique()

# Save the wrong zip codes to a new CSV file
wrong_zip_codes_df = pd.DataFrame(wrong_zip_codes, columns=['Wrong Zip Codes'])
wrong_zip_codes_df.to_csv(output_path, index=False)

print(f"Wrong zip codes saved to {output_path}")
