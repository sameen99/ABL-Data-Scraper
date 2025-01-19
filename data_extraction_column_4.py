import os
import requests
import pandas as pd
import pdfplumber
from io import BytesIO
import logging
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine

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

# Function to extract specific information from the text
def extract_specific_information(text, start_keyword, end_keywords):
    start_index = text.find(start_keyword)
    
    if start_index != -1:
        # Find the end index for each keyword
        end_indices = [text.find(keyword, start_index) for keyword in end_keywords if keyword != -1]
        
        # Extract the text between start and the minimum end index
        min_end_index = min(end_index for end_index in end_indices if end_index != -1)
        if min_end_index != -1:
            return text[start_index + len(start_keyword):min_end_index]
        else:
            return text[start_index:]
    
    return None

# Function to scrape content from the URL
def scrape_content_from_url(fact_sheet_href, start_keyword, end_keywords):
    try:
        # Set a user-agent in the headers to simulate a browser request
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        
        # Download content from URL with headers
        response = requests.get(fact_sheet_href, headers=headers)
        response.raise_for_status()
        
        # Check the content type
        content_type = response.headers.get('content-type', '').lower()
        
        if 'application/pdf' in content_type:
            # Open the PDF from the downloaded content
            with pdfplumber.open(BytesIO(response.content)) as pdf:
                # Loop through all pages in the PDF
                for page_number in range(len(pdf.pages)):
                    page = pdf.pages[page_number]
                    
                    # Extract text from the page
                    text = page.extract_text()
                    
                    # Extract specific information based on criteria
                    specific_info = extract_specific_information(text, start_keyword, end_keywords)
                    
                    # If information is found, append it to the data list
                    if specific_info:
                        data_list.append({'Country': strong_text, 'Type of Sanctions': specific_info})
        else:
            log_message('warning', f"The content type '{content_type}' is not a PDF.")
    
    except Exception as e:
        log_message('error', f"Error in scrape_content_from_url: {e}")

# Function to scrape the website and retrieve articles
def scrape_security_council_reports():
    url = "https://www.securitycouncilreport.org/monthly-forecast/"
    driver = webdriver.Chrome(executable_path=CHROME_DRIVER_PATH)  # Provide path to chromedriver
    driver.get(url)

    try:
        # Get the href from the first element
        first_link_element = driver.find_element(By.XPATH, '//*[@id="publication-stack"]/li[1]/div/h4/a')
        first_link = first_link_element.get_attribute("href")
        
        # Access the link from href
        driver.get(first_link)
        
        # Get href from all elements in the articles section
        article_links_elements = driver.find_elements(By.XPATH, '//*[@id="articles"]/li/h4/a')
        article_links = [element.get_attribute("href") for element in article_links_elements]
        
        # Process each article link
        for article_link in article_links:
            driver.get(article_link)
            
            # Wait for the highlights to load
            highlights_list = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '//*[@id="highlights"]/li'))
            )
            
            # Search for the fact sheet link
            fact_sheet_found = False
            for highlight in highlights_list:
                li_text = highlight.text
                a_tag = highlight.find_element(By.TAG_NAME, 'a')
                a_text = a_tag.text
                if "Fact Sheet on Sanctions" in li_text:
                    fact_sheet_found = True
                    fact_sheet_href = a_tag.get_attribute('href')
                    strong_text = highlight.find_element(By.TAG_NAME, 'strong').text
                    scrape_content_from_url(fact_sheet_href, start_keyword, end_keywords)
                    
    except Exception as e:
        log_message('error', f"Error in scrape_security_council_reports: {e}")
    finally:
        driver.quit()

# Function to insert data into the database
def insert_data_to_db(df):
    try:
        if not df.empty:
            log_message('info', 'Inserting data into the database...')
            engine = create_engine(f'oracle+cx_oracle://{DB_USERNAME}:{DB_PASSWORD}@{DB_DSN}?mode=SYSDBA', max_identifier_length=128)
            df.to_sql('type_of_sanctions', engine, index=False, if_exists='replace')
            log_message('info', 'Data inserted successfully into the database.')
        else:
            log_message('error', 'DataFrame is empty. No data to insert into the database.')
    except Exception as e:
        log_message('error', f"Error inserting data into the database: {e}")
        raise

# Main execution function
def main():
    columns_list = ["Country", "Type of Sanctions"]
    security_council_report_df = pd.DataFrame(columns=columns_list)
    
    try:
        # Scrape data from security council reports
        scrape_security_council_reports()
        
        # Create DataFrame from the data list
        security_council_report_df = pd.DataFrame(data_list)
        
        # Insert data into Oracle DB
        insert_data_to_db(security_council_report_df)
        
    except Exception as e:
        log_message('error', f"An error occurred during execution: {e}")

# Execute the main function
if __name__ == "__main__":
    main()
