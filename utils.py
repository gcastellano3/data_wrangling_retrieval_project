#Importing needed libraries
import requests
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import os

def fetch_with_backoff(url, max_retries=5, base_delay=1, max_delay=60):
    ''' Exponential backoff implementation for robust web scraping '''

    # Try to fetch the URL with retries on failure
    for attempt in range(max_retries):
        try:
            if url == "https://www.transfermarkt.es/statistik/weltrangliste":
                payload = {}
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'es-ES,es;q=0.9',
                    'Referer': 'https://www.transfermarkt.es/'
                }

                # Make the initial request to get the page content
                response = requests.request("GET", url, headers=headers, data=payload, timeout=10)

                # Raise an error for bad responses
                response.raise_for_status()

                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(response.content, "html.parser")

                # Define the table class to locate the relevant data
                soup.find_all(class_='responsive-table')

                # Extract available dates from the dropdown menu
                select = soup.find("select", {"name": "datum"})
                options = select.find_all("option") if select else soup.find_all("option")
                dates = [opt.get("value") for opt in options if opt.get("value")]

                # Filter dates for December only
                selected_dates = [
                    f for f in dates
                    if datetime.strptime(f, "%Y-%m-%d").month == 12
                ]

                # Construct URLs for each selected date
                web_urls = [
                    f'https://www.transfermarkt.es/statistik/weltrangliste/statistik/stat/ajax/yw1/datum/{date}/plus/0/galerie/0'
                    for date in selected_dates
                ]

                # Generate paginated URLs
                pages = []

                for url in web_urls:
                    for pag in range(1, 8):
                        pages.append(f'{url}/page/{pag}')

                # Initialize lists to store extracted data
                years = []
                positions = []
                countries = []
                confeds = []
                points = []

                # Loop through each page and extract the relevant data
                for page in pages:
                    resp = requests.get(page, headers=headers)
                    soup = BeautifulSoup(resp.content, "html.parser")

                    for row in soup.find_all("tr"):
                        tds = row.find_all("td")
                        if len(tds) >= 4:
                            years.append(page.split("/datum/")[1].split("-")[0])
                            positions.append(tds[0].get_text(strip=True))
                            countries.append(tds[1].get_text(strip=True))
                            confeds.append(tds[2].get_text(strip=True))
                            points.append(tds[3].get_text(strip=True))

                # Additionally, extract data for the year 2022
                urls2022 = [
                    f'https://www.transfermarkt.es/statistik/weltrangliste/statistik/stat/ajax/yw1/datum/2022-10-06/plus/0/galerie/0/page/{pag}'
                    for pag in range(1, 8)
                ]
                for page in urls2022:
                    resp = requests.get(page, headers=headers)
                    soup = BeautifulSoup(resp.content, "html.parser")

                    for row in soup.find_all("tr"):
                        tds = row.find_all("td")
                        if len(tds) >= 4:
                            years.append('2022')
                            positions.append(tds[0].get_text(strip=True))
                            countries.append(tds[1].get_text(strip=True))
                            confeds.append(tds[2].get_text(strip=True))
                            points.append(tds[3].get_text(strip=True))

                # Create a DataFrame from the extracted data
                fifa = pd.DataFrame({
                    'Year': years,
                    'Position': positions,
                    'Country': countries,
                    'Confederation': confeds,
                    'Points': points
                })

                # Save the DataFrame to a CSV file
                project_dir = os.getcwd()
                csv_route = os.path.join(project_dir, "data_fifa_ranking.csv")
                fifa.to_csv(csv_route, index=False, encoding="utf-8-sig")
                print("FIFA data saved to 'data_fifa_ranking.csv'")

                return None

            elif url == "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv":
                # 1. Cargar dataset mundial de PIB
                pib = pd.read_csv(url)

                # 2. Conversión de USD a EUR
                # Definimos un tipo de cambio aproximado actual
                usd_to_eur = 0.93  # ajusta según convenga

                # Creamos columna con PIB en euros
                pib['Value_EUR'] = pib['Value'] * usd_to_eur

                # Y en billones de euros (para más fácil lectura)
                pib['Value_EUR_billions'] = pib['Value_EUR'] / 1e9

                # 3. Guardar CSV limpio
                # Guardar en la misma carpeta que el script
                project_dir = os.getcwd()
                csv_route = os.path.join(project_dir, "data_pib_mundial.csv")
                pib.to_csv(csv_route, index=False, encoding="utf-8-sig")
                print("PIB data saved to 'data_pib_mundial.csv'")

                return None

            else:
                print("URL not recognized for data processing.")

        except (requests.RequestException, requests.Timeout) as e:

            # Calculate exponential backoff delay
            wait_time = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)

            # Log the error
            print(f"Attempt {attempt + 1} failed: {e}. Waiting {wait_time:.2f}s before retrying...")

            # Wait before the next retry
            time.sleep(wait_time)
    
    # If all retries fail, log the failure
    print("Too many retries. Could not retrieve the page.")
    return None


def data_wrangling():
    ''' Data wrangling and merging FIFA and PIB datasets '''

    # Creating dataframes for each file
    fifa = pd.read_csv('data_fifa_ranking.csv')
    pib = pd.read_csv('data_pib_mundial.csv')
    countries = pd.read_csv('mapping_countries.csv')

    # Renaming columns for merging
    fifa = fifa.rename(columns={'Country':'Country_ESP'})
    pib = pib.rename(columns={'Country Name':'Country_ENG'})
    countries = countries.rename(columns={' name':'Country_ENG', 'nombre':'Country_ESP', ' iso3':'iso3', ' nom':'Country_FRA'})

    # Replacing country names in fifa to match those in countries.csv
    countries_replacements = {
        'Tunez': 'Túnez',
        'Reino Unido': 'Inglaterra',
        'Estados Unidos de América': 'Estados Unidos',
        'Qatar': 'Catar',
        'Islas Bermudas': 'Bermudas',
        'Emiratos Árabes Unidos': 'EAU',
        'Trinidad y Tobago': 'Trinidad',
        'Mali': 'Malí',
        'Malawi': 'Malaui',
        'Bahrein': 'Baréin',
        'República del Congo': 'Congo',
        'Surinám': 'Surinam',
        'Niger': 'Níger',
        'San Vicente y las Granadinas': 'San Vicente',
        'Antigua y Barbuda': 'Antigua y Barb.',
        'Guinea-Bissau': 'Guinea-Bisáu',
        'Hong kong': 'Hong Kong',
        'Bangladesh': 'Bangladés',
        'República Dominicana': 'Rep. Dominicana',
        'República Centroafricana': 'Centroáfrica',
        'Guinea Ecuatorial': 'Guinea Ecuat.',
        'Birmania': 'Myanmar',
        'Islas Maldivas': 'Maldivas',
        'Sri lanka': 'Sri Lanka',
        'San Cristóbal y Nieves': 'St. Kitts/Nevis',
        'República Checa': 'Chequia',
        'Macedônia': 'Macedonia Norte',
        'Bosnia y Herzegovina': 'Bosnia',
        'Papúa Nueva Guinea': 'Papúa N. Guinea',
        'República Democrática del Congo': 'RD Congo',
        'Islas Vírgenes Británicas': 'Vírgenes B.',
        'Islas Turcas y Caicos': 'Turcas y Caicos',
        'Santo Tomé y Príncipe': 'Santo Tomé y P.',
        'República de Sudán del Sur': 'Sudán del Sur',
        'Samoa Americana': 'Samoa A.',
        'Islas Vírgenes de los Estados Unidos': 'Vírgenes A.'
    }
    countries['Country_ESP'] = countries['Country_ESP'].replace(countries_replacements)

    # Merging fifa and country code datasets
    fifa = pd.merge(
        fifa,
        countries[['Country_ESP', 'iso3']],
        on='Country_ESP',
        how='left'
        )

    # Renaming columns for merging
    pib = pib.rename(columns={'Country Code':'iso3'})

    # Final merge
    fifa_pib = pd.merge(
        fifa,
        pib,
        on=['iso3', 'Year'],
        how='left'
        )

    # Removing rows with missing iso3 codes
    fifa_pib = fifa_pib[fifa_pib['iso3'].notna()]

    return fifa_pib