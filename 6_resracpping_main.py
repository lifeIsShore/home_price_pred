import os
import csv
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(filename="scraping_debug.log", level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Path to the ZIP codes file (merged_rescrapping.csv)
zip_codes_file = r"C:\Users\ahmty\Desktop\projects\price-pred\rescrapping_dataframe\merged_rescrapping\merged_rescrapping.csv"

# Read ZIP codes from the merged file
zip_codes = []
city_names = []
with open(zip_codes_file, mode="r", encoding="utf-8-sig") as file:
    reader = csv.reader(file)
    header = next(reader, None)  # Skip header if present
    for row in reader:
        zip_codes.append(row[0])  # Read all ZIP codes
        if len(row) > 1:  # If the second column exists, get the city name
            city_names.append(row[1])
        else:
            city_names.append(None)  # If no city, append None

# Function to scrape prices for a given ZIP code
def scrape_prices(zip_code, city_name):
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    logging.info(f"Processing ZIP code: {zip_code}")

    try:
        driver.get("https://www.immowelt.de/immobilienpreise/deutschland")
        wait = WebDriverWait(driver, 20)
        time.sleep(random.uniform(2, 4))

        # Wait for search input
        input_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Suche nach Stadt, Adresse ..."]')))
        input_field.clear()
        time.sleep(random.uniform(1, 2))
        input_field.send_keys(zip_code)
        time.sleep(random.uniform(1, 2))
        input_field.send_keys(Keys.RETURN)

        # Wait for price elements
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-u7w3u5')))
        time.sleep(random.uniform(4, 6))

        # Extract price elements
        price_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.css-u7w3u5')))

        wohnung_min = price_elements[0].text.replace('€', '').replace('.', '').strip() if len(price_elements) > 0 else "N/A"
        wohnung_max = price_elements[1].text.replace('€', '').replace('.', '').strip() if len(price_elements) > 1 else "N/A"
        haus_min = price_elements[2].text.replace('€', '').replace('.', '').strip() if len(price_elements) > 2 else "N/A"
        haus_max = price_elements[3].text.replace('€', '').replace('.', '').strip() if len(price_elements) > 3 else "N/A"

        return [zip_code, city_name, wohnung_min, wohnung_max, haus_min, haus_max]

    except Exception as e:
        logging.error(f"Error processing ZIP {zip_code}: {e}", exc_info=True)
        return None

    finally:
        driver.quit()
        time.sleep(random.uniform(3, 5))

# Output folder and final file
scraped_data_folder = r"C:\Users\ahmty\Desktop\projects\price-pred\scraped_data"
os.makedirs(scraped_data_folder, exist_ok=True)
output_file = os.path.join(scraped_data_folder, "corrected_zip.csv")

# Process ZIP codes using multithreading
max_threads = 5
results_buffer = []

with ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = {executor.submit(scrape_prices, zip_code, city_name): (zip_code, city_name) for zip_code, city_name in zip(zip_codes, city_names)}
    for future in as_completed(futures):
        result = future.result()
        if result:
            results_buffer.append(result)
            print(f"✅ Scraped: {result}")

# Save the final scraped data
with open(output_file, mode='w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["Zip Code", "City", "Wohnung Price min(€)", "Wohnung Price max(€)", "Haus Price min(€)", "Haus Price max(€)"])
    writer.writerows(results_buffer)

print(f"✅ Scraping completed! Data saved in {output_file}")
