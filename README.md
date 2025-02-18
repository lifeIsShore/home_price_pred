# Home Price Prediction (`home_price_pred`)

A web scraping project to collect and analyze real estate price data, calculating the average price per square meter.

## Features

- Uses multi-threading for efficiency.
- Scrapes real estate price data.
- Automatically detects and fixes missing or incorrect records.
- Runs recursively until all data is correctly collected.

## How It Works

1. The program starts with a list of postal codes.
2. It creates 5 threads, processing batches of 10 postal codes at a time.
3. For each postal code, the program scrapes:
   - **Wohnung (Apartment)** Min Price
   - **Wohnung (Apartment)** Max Price
   - **Haus (House)** Min Price
   - **Haus (House)** Max Price
4. After scraping, the program checks for missing or incorrect records.
5. The collected data is stored in a directory.
6. The program merges the data.
7. If there are missing or incorrect records, the program repeats the web scraping process.
8. Data is saved, either replacing incorrect records or appending new ones.
9. Steps 7–8 are repeated recursively until all records are complete and accurate.

## Requirements

- Python 3.x
- Required libraries:
  - `requests`
  - `BeautifulSoup`
  - `pandas`
  - `threading`

## Installation

```bash
git clone https://github.com/yourusername/home_price_pred.git
cd home_price_pred
pip install -r requirements.txt
