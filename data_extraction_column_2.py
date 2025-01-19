import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')
CHROME_DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH')

# Function to log messages with different levels
def log_message(level, message):
    if level == 'info':
        print(f"[INFO] {message}")
    elif level == 'debug':
        print(f"[DEBUG] {message}")
    elif level == 'error':
        print(f"[ERROR] {message}")

# Initialize WebDriver
def init_driver():
    try:
        log_message('info', 'Initializing Chrome WebDriver...')
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")  # Run in headless mode

        service = Service(CHROME_DRIVER_PATH)
        service.start()
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        log_message('info', 'Chrome WebDriver initialized successfully.')
        return driver
    except Exception as e:
        log_message('error', f'Error initializing WebDriver: {e}')
        raise

# Scraping data from the website
def scrape_data(driver):
    try:
        url = 'https://www.sanctionsmap.eu/#/main'
        driver.get(url)
        log_message('info', f'Navigating to {url}...')

        ul_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'filter-list'))
        )
        log_message('info', 'Elements loaded successfully.')

        # Prepare DataFrame
        columns_list = ["Country", "Restrictive Measures", "Executive Orders", "Type of Sanctions"]
        main_df = pd.DataFrame(columns=columns_list)
        main_df['Executive Orders'] = None
        main_df['Type of Sanctions'] = None

        # Scrape each country's data
        for ul in ul_elements:
            country_element = ul.find_element(By.CSS_SELECTOR, 'li[data-heading="Country or Category"] div a')
            country_text = country_element.text

            restrictiveMeasure_element = ul.find_element(By.CSS_SELECTOR, 'li[data-heading="Restrictive measures"]')
            child_li_elements = restrictiveMeasure_element.find_elements(By.TAG_NAME, 'li')
            
            for index, li_ele in enumerate(child_li_elements):
                div_element = li_ele.find_element(By.TAG_NAME, 'div')
                div_element.click()

                try:
                    restrictiveMeasure_div_element = driver.find_element(By.CLASS_NAME, 'popover-content')
                    Restrictive_measure_text = restrictiveMeasure_div_element.text
                    
                    row_object = {"Country": country_text, "Restrictive Measure": Restrictive_measure_text}
                    main_df = pd.concat([main_df, pd.DataFrame([row_object])], ignore_index=True)

                except NoSuchElementException:
                    log_message('error', f"Div with class 'popover-content' not found for country {country_text}")

        return main_df
    except Exception as e:
        log_message('error', f'Error during data scraping: {e}')
        raise

# Insert data into the Oracle database
def insert_data_to_db(df):
    try:
        if not df.empty:
            log_message('info', 'Inserting data into the database...')
            engine = create_engine(f'oracle+cx_oracle://{DB_USERNAME}:{DB_PASSWORD}@{DB_DSN}?mode=SYSDBA', max_identifier_length=128)
            df.to_sql('restrictive_measure', engine, index=False, if_exists='replace')
            log_message('info', 'Data inserted successfully into the database.')
        else:
            log_message('error', 'DataFrame is empty. No data to insert into the database.')
    except Exception as e:
        log_message('error', f'Error inserting data into the database: {e}')
        raise

# Main execution function
def main():
    try:
        driver = init_driver()
        main_df = scrape_data(driver)
        insert_data_to_db(main_df)
    finally:
        log_message('debug', 'Closing WebDriver...')
        driver.quit()

# Execute the main function
if __name__ == "__main__":
    main()
