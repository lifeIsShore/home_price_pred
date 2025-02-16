from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import csv
import os

# Initialize the browser (e.g., Chrome)
driver = webdriver.Chrome()  # Make sure you have ChromeDriver installed

# Open the website
driver.get("https://www.immowelt.de/immobilienpreise/deutschland")  # Replace with the actual website URL

# Function to input zip code, scrape prices, and save to CSV
def scrape_prices(zip_code, city, state):
    try:
        # Locate the input field and input the zip code
        input_field = driver.find_element(By.CSS_SELECTOR, 'input[aria-label="Suche nach Stadt, Adresse ..."]')
        input_field.clear()  # Clear any existing text
        input_field.send_keys(zip_code)  # Input the zip code
        input_field.send_keys(Keys.RETURN)  # Press Enter

        # Wait for the page to update (adjust the sleep time as needed)
        time.sleep(5)

        # Scrape the Wohnung price
        wohnung_price_element = driver.find_element(By.CSS_SELECTOR, 'span.css-2bd70b:nth-of-type(1)')
        wohnung_price = wohnung_price_element.text.replace('€', '').replace('.', '').replace(',', '.').strip()

        # Scrape the Haus price
        haus_price_element = driver.find_element(By.CSS_SELECTOR, 'span.css-2bd70b:nth-of-type(2)')
        haus_price = haus_price_element.text.replace('€', '').replace('.', '').replace(',', '.').strip()

        # Save the data to a CSV file
        output_file = os.path.join(output_dir, 'scraped_prices.csv')
        with open(output_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow([zip_code, city, state, wohnung_price, haus_price])

        print(f"Zip Code: {zip_code}, City: {city}, State: {state}, Wohnung: {wohnung_price} €, Haus: {haus_price} €")
    except Exception as e:
        print(f"Error processing zip code {zip_code}: {e}")

# Paths
input_csv = r"C:\Users\ahmty\Desktop\projects\price-pred\german-postcodes.csv"
output_dir = r"C:\Users\ahmty\Desktop\projects\price-pred\zip-codes-m2\db-postcodes"

# Create or clear the output CSV file and write the header
output_file = os.path.join(output_dir, 'scraped_prices.csv')
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["Zip Code", "City", "State", "Wohnung Price (€)", "Haus Price (€)"])

# Read the input CSV and process each zip code
with open(input_csv, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip the header row
    for row in reader:
        zip_code = row[1]  # Assuming the zip code is in the second column
        city = row[0]      # Assuming the city is in the first column
        state = row[2]     # Assuming the state is in the third column
        scrape_prices(zip_code, city, state)

# Close the browser
driver.quit()