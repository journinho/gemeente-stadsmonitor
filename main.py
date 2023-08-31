from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import pandas as pd

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    else:
        print(f"Failed to get data from {url}")
        return None

def process_page(soup, base_url):
    data = []
    for div in soup.find_all('div', class_='views-row'):
        a = div.find('a')
        if a:
            indicator_url = base_url + a['href']
            indicator_soup = fetch_data(indicator_url)
            
            if indicator_soup:
                row = {}
                for field_div in indicator_soup.find_all('div', class_='field'):
                    label = field_div.find('div', class_='field__label').text.strip()
                    value = field_div.find('div', class_='field__item').text.strip()
                    row[label] = value
                
                download_url = indicator_soup.find('a', text='Download de Excel dataset voor deze indicator')['href']
                indicator_div = indicator_soup.find('div', class_='taxonomy-term__detail')
                title = indicator_div.find('h2').text.strip()
                
                row['Title'] = title
                row['Download URL'] = download_url
                
                data.append(row)
    return data

# Replace this with the base URL of the website
base_url = 'https://gemeente-stadsmonitor.vlaanderen.be'
urls = [
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=925&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=926&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=927&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2295&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2297&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2298&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2299&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2301&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2302&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2303&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=3&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2307&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=17997&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=17997&field_type_value=All&name=&page=1",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=17997&field_type_value=All&name=&page=2",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2294&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2294&field_type_value=All&name=&page=1",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2304&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2304&field_type_value=All&name=&page=1",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2296&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2296&field_type_value=All&name=&page=1",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2305&field_type_value=All&name=",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2305&field_type_value=All&name=&page=1",
    "https://gemeente-stadsmonitor.vlaanderen.be/indicatoren?field_thema_target_id=2305&field_type_value=All&name=&page=2"

]

# Fetch and parse in parallel
all_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    soups = list(executor.map(fetch_data, urls))
    
    for soup in soups:
        if soup:
            all_data.extend(process_page(soup, base_url))

# Create DataFrame
df = pd.DataFrame(all_data)
df = df.drop_duplicates(subset='Download URL')

# Reset index and save
df = df.reset_index(drop=True)
df.to_csv('gsm.csv', sep=";")
df.to_json('gsm.json')
