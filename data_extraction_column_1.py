import os
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# Retrieve credentials from environment variables
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')

# Define the table name
TABLE_NAME = 'country'

# List of countries 
countries = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan", 
    "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Burma", "Benin", "Bhutan", "Bolivia", "Bosnia & Herzegovina", 
    "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Chile", "China", "Colombia", 
    "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark", "Djibouti", "Hong Kong", "Dominica", "Dominican Republic", 
    "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon", "Germany", 
    "Greece", "Guatemala", "Guinea", "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Ireland", "Israel", "Italy", "Japan", 
    "Jordan", "Kazakhstan", "Kenya", "Kuwait", "Kyrgyzstan", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Lithuania", "Luxembourg", "Malawi", "Malaysia",
    "Malta", "Mauritius", "Mexico", "Morocco", "Mozambique", "Myanmar", "Nepal", "Netherlands", "Nigeria", "Pakistan", "Panama", "Peru", "Poland", "Portugal",
    "Qatar", "Romania", "Russia", "Serbia", "Singapore", "South Africa", "Spain", "Sri Lanka", "Sweden", "Switzerland", "Syria", "Taiwan", "Thailand", "Turkey", 
    "Ukraine", "United Kingdom", "United States", "Uzbekistan", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
]

# Clean up and remove duplicates
countries = list(set(countries))

# Create the DataFrame
df_countries = pd.DataFrame(countries, columns=['Country'])

# Remove whitespaces from 'Country' column
df_countries['Country'] = df_countries['Country'].str.strip()

# Function to log messages with different levels
def log_message(level, message):
    if level == 'info':
        print(f"[INFO] {message}")
    elif level == 'debug':
        print(f"[DEBUG] {message}")
    elif level == 'error':
        print(f"[ERROR] {message}")

# Try to connect and insert data into the database
try:
    log_message('info', 'Attempting to connect to the Oracle database...')
    
    # Establish a connection using SQLAlchemy and cx_Oracle
    engine = create_engine(f'oracle+cx_oracle://{DB_USERNAME}:{DB_PASSWORD}@{DB_DSN}?mode=SYSDBA', max_identifier_length=128)
    
    log_message('info', 'Successfully connected to the database.')

    # Check if the dataframe is not empty
    if not df_countries.empty:
        log_message('info', 'Inserting countries data into the table...')
        
        # Insert the cleaned DataFrame into the Oracle database
        df_countries.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        
        log_message('info', f'Data successfully inserted into the {TABLE_NAME} table.')
    else:
        log_message('error', 'The DataFrame is empty, no data to insert.')

except Exception as e:
    log_message('error', f'An error occurred: {e}')

finally:
    log_message('debug', 'Process completed.')
