from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to input zip code, scrape prices, and return results
def scrape_prices(zip_code, city, state):
    try:
        # Initialize a new browser instance for each thread
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in headless mode (no GUI)
        driver = webdriver.Chrome(options=options)  # Make sure you have ChromeDriver installed
        driver.get("https://www.immowelt.de/immobilienpreise/deutschland")

        # Wait for the page to load
        time.sleep(3)

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

        # Close the browser instance
        driver.quit()

        # Return the results
        return [zip_code, city, state, wohnung_price, haus_price]
    except Exception as e:
        print(f"Error processing zip code {zip_code}: {e}")
        driver.quit()  # Ensure the browser is closed even if an error occurs
        return None

# Function to save results to CSV
def save_results(results, output_file):
    with open(output_file, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(results)

# Paths
input_csv = r"C:\Users\ahmty\Desktop\projects\price-pred\german-postcodes.csv"
output_dir = r"C:\Users\ahmty\Desktop\projects\price-pred\zip-codes-m2"

# Create or clear the output CSV file and write the header
output_file = os.path.join(output_dir, 'scraped_prices.csv')
with open(output_file, mode='w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["Zip Code", "City", "State", "Wohnung Price (€)", "Haus Price (€)"])

# Read the input CSV and prepare the data
zip_codes = []
with open(input_csv, mode='r', encoding='utf-8-sig') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip the header row
    for row in reader:
        zip_codes.append((row[1], row[0], row[2]))  # (Zip Code, City, State)

# Use ThreadPoolExecutor to process zip codes in parallel (10 threads)
results_buffer = []  # Buffer to store results before saving
record_count = 0     # Counter to track the number of records processed

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(scrape_prices, zip_code, city, state) for zip_code, city, state in zip_codes]

    # Save results as they are completed
    for future in as_completed(futures):
        result = future.result()
        if result:
            results_buffer.append(result)
            record_count += 1

            # Save every 100 records
            if record_count % 100 == 0:
                for record in results_buffer:
                    save_results(record, output_file)
                results_buffer = []  # Clear the buffer after saving
                print(f"Saved {record_count} records so far...")

    # Save any remaining records in the buffer
    if results_buffer:
        for record in results_buffer:
            save_results(record, output_file)
        print(f"Saved {record_count} records in total.")

print("Scraping completed!")