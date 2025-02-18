import pandas as pd

# File paths
corrected_zip_path = r"C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\corrected_zip.csv"
merged_data_path = r"C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged\merged_data_BEFORE_control.csv"
output_path = r"C:\Users\ahmty\Desktop\projects\price-pred\scraped_data\merged\merged_data_AFTER_control.csv"

# Load corrected zip data
corrected_df = pd.read_csv(corrected_zip_path, sep=';')

# Load merged data before control
merged_df = pd.read_csv(merged_data_path, sep=',')

# Convert ZIP codes to string for proper matching
corrected_df['Zip Code'] = corrected_df['Zip Code'].astype(str)
merged_df['Zip Code'] = merged_df['Zip Code'].astype(str)

# Create a mapping dictionary from corrected zip file
correction_dict = corrected_df.set_index('Zip Code')[["Wohnung Price min(€)", "Wohnung Price max(€)", "Haus Price min(€)", "Haus Price max(€)"]].to_dict(orient="index")

# Update merged data where ZIP codes match
for index, row in merged_df.iterrows():
    zip_code = row['Zip Code']
    if zip_code in correction_dict:
        merged_df.at[index, merged_df.columns[-4]] = correction_dict[zip_code]["Wohnung Price min(€)"]
        merged_df.at[index, merged_df.columns[-3]] = correction_dict[zip_code]["Wohnung Price max(€)"]
        merged_df.at[index, merged_df.columns[-2]] = correction_dict[zip_code]["Haus Price min(€)"]
        merged_df.at[index, merged_df.columns[-1]] = correction_dict[zip_code]["Haus Price max(€)"]

# Save updated data
merged_df.to_csv(output_path, index=False, sep=',')

print(f"✅ Updated file saved as {output_path}")
