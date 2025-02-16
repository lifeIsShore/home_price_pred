from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import os
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging setup
logging.basicConfig(filename="scraping_errors.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to scrape prices for a given zip code
def scrape_prices(zip_code, city, state):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)

    try:
        driver.get("https://www.immowelt.de/immobilienpreise/deutschland")
        wait = WebDriverWait(driver, 10)

        # Locate and input the zip code
        input_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Suche nach Stadt, Adresse ..."]')))
        input_field.clear()
        input_field.send_keys(zip_code, Keys.RETURN)

        # Wait until price elements appear
        wohnung_price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span.css-2bd70b:nth-of-type(1)')))
        haus_price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span.css-2bd70b:nth-of-type(2)')))

        wohnung_price = wohnung_price_element.text.replace('€', '').replace('.', '').replace(',', '.').strip()
        haus_price = haus_price_element.text.replace('€', '').replace('.', '').replace(',', '.').strip()

        return [zip_code, city, state, wohnung_price, haus_price]
    except Exception as e:
        logging.error(f"Error processing zip code {zip_code}: {e}")
        return None
    finally:
        driver.quit()
        time.sleep(random.uniform(2, 5))  # Random delay to prevent detection

# Function to save results to CSV safely
def save_results(results, output_file):
    with open(output_file, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(results)

# Paths
input_csv = r"C:\Users\ahmty\Desktop\projects\price-pred\german-postcodes.csv"
output_dir = r"C:\Users\ahmty\Desktop\projects\price-pred\zip-codes-m2"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Create or clear the output CSV file and write the header
output_file = os.path.join(output_dir, 'scraped_prices.csv')
with open(output_file, mode='w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["Zip Code", "City", "State", "Wohnung Price (€)", "Haus Price (€)"])

# Read the input CSV and prepare the zip code data
zip_codes = []
with open(input_csv, mode='r', encoding='utf-8-sig') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip the header row
    for row in reader:
        zip_codes.append((row[1], row[0], row[2]))  # (Zip Code, City, State)

# Multi-threaded execution (10 threads)
results_buffer = []
record_count = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(scrape_prices, zip_code, city, state) for zip_code, city, state in zip_codes]

    for future in as_completed(futures):
        result = future.result()
        if result:
            results_buffer.append(result)
            record_count += 1

            # Save every 100 records
            if record_count % 100 == 0:
                for record in results_buffer:
                    save_results(record, output_file)
                results_buffer = []  # Clear buffer
                print(f"Saved {record_count} records so far...")

    # Save any remaining records in the buffer
    if results_buffer:
        for record in results_buffer:
            save_results(record, output_file)
        print(f"Saved {record_count} records in total.")

print("Scraping completed!")
