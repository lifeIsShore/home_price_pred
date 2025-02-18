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

# Load ZIP code dataset into a dictionary
zip_code_dict = {}
with open(r"C:\Users\ahmty\Desktop\projects\price-pred\filtered_dataset.csv", mode="r", encoding="utf-8-sig") as file:
    reader = csv.reader(file, delimiter=";")
    next(reader)  # Skip the header
    for row in reader:
        plz, name = row
        zip_code_dict[plz] = name

# Function to scrape prices for a given ZIP code
def scrape_prices(zip_code):
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    logging.info(f"Processing ZIP code: {zip_code}")

    try:
        driver.get("https://www.immowelt.de/immobilienpreise/deutschland")
        wait = WebDriverWait(driver, 20)  # Increased wait time
        
        time.sleep(random.uniform(2, 4))  # Wait before interacting
        
        # Wait for input field to be present and interact
        input_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Suche nach Stadt, Adresse ..."]')))
        input_field.clear()
        time.sleep(random.uniform(1, 2))  # Give time before input
        input_field.send_keys(zip_code)
        time.sleep(random.uniform(1, 2))  # Slow down interaction
        input_field.send_keys(Keys.RETURN)
        logging.info(f"Entered ZIP code {zip_code} and submitted search.")

        # Wait for results page to load
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-u7w3u5')))
        time.sleep(random.uniform(4, 6))  # Extra wait for page to load

        # Extract all price elements
        retries = 3
        for attempt in range(retries):
            try:
                price_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.css-u7w3u5')))
                if len(price_elements) >= 4:
                    break
                time.sleep(random.uniform(2, 4))  # Retry delay
            except Exception:
                logging.warning(f"Retry {attempt+1} for ZIP {zip_code}")

        # Assign values based on order
        wohnung_min = price_elements[0].text.replace('€', '').replace('.', '').replace(',', '.').strip() if len(price_elements) > 0 else "N/A"
        wohnung_max = price_elements[1].text.replace('€', '').replace('.', '').replace(',', '.').strip() if len(price_elements) > 1 else "N/A"
        haus_min = price_elements[2].text.replace('€', '').replace('.', '').replace(',', '.').strip() if len(price_elements) > 2 else "N/A"
        haus_max = price_elements[3].text.replace('€', '').replace('.', '').replace(',', '.').strip() if len(price_elements) > 3 else "N/A"

        # Retrieve city name from dictionary
        city = zip_code_dict.get(zip_code, "Unknown")

        return [zip_code, city, wohnung_min, wohnung_max, haus_min, haus_max]

    except Exception as e:
        logging.error(f"Error processing ZIP {zip_code}: {e}", exc_info=True)
        return None

    finally:
        driver.quit()
        time.sleep(random.uniform(3, 5))  # Increased delay before next request

# **Create 'scraped_data' folder if it doesn't exist**
scraped_data_folder = r"C:\Users\ahmty\Desktop\projects\price-pred\scraped_data"
os.makedirs(scraped_data_folder, exist_ok=True)

# Read ZIP codes from dataset
zip_codes = list(zip_code_dict.keys())  # List of ZIP codes

# **Process in batches of 10**
batch_size = 10
max_threads = 5  # 10 simultaneous threads

for batch_index, i in enumerate(range(0, len(zip_codes), batch_size), start=1):
    batch = zip_codes[i:i+batch_size]  # Get the next 10 ZIP codes
    results_buffer = []

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(scrape_prices, zip_code): zip_code for zip_code in batch}

        for future in as_completed(futures):
            result = future.result()
            if result:
                results_buffer.append(result)
                print(f"✅ Scraped: {result}")

    # **Save each batch separately into 'scraped_data' folder**
    batch_file = os.path.join(scraped_data_folder, f"batch_{batch_index}.csv")
    with open(batch_file, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(["Zip Code", "City", "Wohnung Price min(€)", "Wohnung Price max(€)", "Haus Price min(€)", "Haus Price max(€)"])
        writer.writerows(results_buffer)

    print(f"✅ Saved Batch {batch_index} to {batch_file}")

print("✅ Scraping completed! Check 'scraping_debug.log' for details.")