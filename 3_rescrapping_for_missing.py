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

# Configure logging
logging.basicConfig(filename="scraping_errors.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to scrape prices for a given zip code
def scrape_prices(zip_code, city, state):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.immowelt.de/immobilienpreise/deutschland")
        wait = WebDriverWait(driver, 10)
        
        input_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Suche nach Stadt, Adresse ..."]')))
        input_field.clear()
        input_field.send_keys(zip_code, Keys.RETURN)
        
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

# Function to save results to CSV
def save_results(results, output_file):
    with open(output_file, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows(results)

# Define input and output file paths
input_csv = r"C:\Users\ahmty\Desktop\projects\price-pred\german-postcodes.csv"
output_dir = r"C:\Users\ahmty\Desktop\projects\price-pred\zip-codes-m2"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, 'scraped_prices.csv')

# Create or reset the output CSV file
with open(output_file, mode='w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["Zip Code", "City", "State", "Wohnung Price (€)", "Haus Price (€)"])

# Read zip codes from input CSV
zip_codes = []
with open(input_csv, mode='r', encoding='utf-8-sig') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip the header row
    for row in reader:
        zip_codes.append((row[1], row[0], row[2]))  # (Zip Code, City, State)

# Process zip codes in batches of 10 with 10 threads at a time
batch_size = 10
batch_number = 0

for i in range(0, len(zip_codes), batch_size * 10):  # 10 threads, each handling 10 records
    batch_number += 1
    batch_records = zip_codes[i:i + (batch_size * 10)]
    results_buffer = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(scrape_prices, zip_code, city, state) for zip_code, city, state in batch_records]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                results_buffer.append(result)
    
    if results_buffer:
        save_results(results_buffer, output_file)
        print(f"Batch number {batch_number} is saved.")

print("Scraping completed!")
