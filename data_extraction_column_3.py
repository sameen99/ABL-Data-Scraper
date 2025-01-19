import os
import requests
import pandas as pd
import urllib.request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import logging

# Load environment variables
load_dotenv()

# Retrieve credentials and paths from .env file
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')
CHROME_DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH')

# Setup logging configuration
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Log function to standardize logging
def log_message(level, message):
    if level == 'info':
        logging.info(message)
    elif level == 'debug':
        logging.debug(message)
    elif level == 'error':
        logging.error(message)
    else:
        logging.warning(message)

# Function to scrape active sanction programs
def scrape_active_sanction_programs(base_url):
    driver = webdriver.Chrome(executable_path=CHROME_DRIVER_PATH)

    try:
        log_message('info', f'Navigating to {base_url}...')
        driver.get(base_url)

        program_links = WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.XPATH, '//table[@class="table cols-2"]//tbody//tr//td[1]/a'))
        )

        data = []  # List to store data for DataFrame
        log_message('info', 'Scraping program links...')
        for link in program_links:
            program_name = link.text
            program_url = link.get_attribute("href")
            log_message('debug', f"Scraping orders for {program_name} at {program_url}")

            # Call the modified scrape_executive_orders function
            orders_data = scrape_executive_orders(program_url, program_name)
            data.extend(orders_data)

        # Create a DataFrame
        df = pd.DataFrame(data, columns=["Country", "Executive Orders"])
        log_message('info', 'Scraping complete. Returning the DataFrame...')
        return df

    except Exception as e:
        log_message('error', f"Error in scrape_active_sanction_programs: {e}")
        raise

    finally:
        driver.quit()

# Function to scrape executive orders
def scrape_executive_orders(url, program_name):
    try:
        log_message('info', f"Scraping executive orders from {url}...")
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            heading = soup.find('h3', string=lambda text: text and 'Sanctions Brochures' in text)

            orders_data = []  # List to store data for DataFrame
            if heading:
                ul_tag = heading.find_next('ul')

                if ul_tag:
                    a_tag = ul_tag.find('a')

                    if a_tag:
                        pdf_link = a_tag.get('href')
                        full_url = 'https://ofac.treasury.gov' + pdf_link
                        orders_data.append([program_name, full_url])
                        log_message('info', f"Found brochure link for {program_name}: {full_url}")
            
            # Scrape executive orders if available
            heading1 = soup.find('h4', string=lambda text: text and 'Executive Orders' in text)
            if heading1:
                ul_tag = heading1.find_next('ul')

                if ul_tag:
                    for li_tag in ul_tag.find_all('li'):
                        a_tag = li_tag.find('a')
                        if a_tag:
                            pdf_link = a_tag.get('href')
                            full_url = 'https://ofac.treasury.gov' + pdf_link
                            order_title = li_tag.text.strip()
                            orders_data.append([program_name, f"{order_title} {full_url}"])
                            log_message('info', f"Found executive order for {program_name}: {order_title} {full_url}")

            return orders_data
        else:
            log_message('error', f"Failed to retrieve {url} with status code {response.status_code}")
            return []

    except Exception as e:
        log_message('error', f"Error in scrape_executive_orders: {e}")
        return []

# Function to insert data into the database
def insert_data_to_db(df):
    try:
        if not df.empty:
            log_message('info', 'Inserting data into the database...')
            engine = create_engine(f'oracle+cx_oracle://{DB_USERNAME}:{DB_PASSWORD}@{DB_DSN}?mode=SYSDBA', max_identifier_length=128)
            df.to_sql('executive_orders', engine, index=False, if_exists='replace')
            log_message('info', 'Data inserted successfully into the database.')
        else:
            log_message('error', 'DataFrame is empty. No data to insert into the database.')
    except Exception as e:
        log_message('error', f"Error inserting data into the database: {e}")
        raise

# Main execution function
def main():
    base_url = 'https://ofac.treasury.gov/sanctions-programs-and-country-information'
    try:
        # Scrape data from active sanction programs
        df = scrape_active_sanction_programs(base_url)
        
        # Insert data into Oracle DB
        insert_data_to_db(df)

    except Exception as e:
        log_message('error', f"An error occurred during execution: {e}")

# Execute the main function
if __name__ == "__main__":
    main()
