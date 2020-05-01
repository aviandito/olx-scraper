# Reference: https://hackernoon.com/building-a-web-scraper-from-start-to-finish-bb6b95388184
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
import sqlite3
import time
import random

def download_html(url):
    print('Downloading html from ' + url)
    response = requests.get(url, timeout = 5)
    content = BeautifulSoup(response.content, 'html.parser')

    return content

def scrape_rumah(content):
    print('Start scraping from html tags...')

    scraped = []
    ad_object = {}

    now = datetime.now()

    for ad in content.findAll('div', attrs={"class": "IKo3_"}):
        ad_object = {
            'scrape_timestamp_wib': now.isoformat(),
            'title': ad.find('span', attrs={"data-aut-id": "itemTitle"}).text.lower(),
            'bedroom': int(re.sub('[^0-9]', '', re.split(' - ', ad.find('span', attrs={"data-aut-id": "itemDetails"}).text)[0])),
            'bathroom': int(re.sub('[^0-9]', '', re.split(' - ', ad.find('span', attrs={"data-aut-id": "itemDetails"}).text)[1])),
            'land_area': int(re.sub('[^0-9]m2', '', re.split(' - ', ad.find('span', attrs={"data-aut-id": "itemDetails"}).text)[2])),
            'price': int(re.sub('[^0-9]', '', ad.find('span', attrs={"data-aut-id": "itemPrice"}).text)),
            'kecamatan': re.split(', ', ad.find('span', attrs={"data-aut-id": "item-location"}).text)[0],
            'kota_kab': re.split(', ', ad.find('span', attrs={"data-aut-id": "item-location"}).text)[1],
            'post_date': ad.find('span', attrs={"class": "zLvFQ"}).text
        }
        scraped.append(ad_object)

    entry_url_list = [entry_url.findChild('a')['href'] for entry_url in content.findAll('li', attrs={"data-aut-id": "itemBox"})]
    img_url_list = [figure.findChild('img')['src'] for figure in content.findAll('figure', attrs={"data-aut-id": "itemImage"})]

    for i in range(len(scraped)):
        scraped[i]['entry_url'] = 'https://www.olx.co.id' + entry_url_list[i]
        scraped[i]['img_url'] = img_url_list[i]
        scraped[i]['rumah_id'] = int(re.split('-', scraped[i]['entry_url'])[-1])
    
    return scraped

def append_rumah_to_sqlite(db_name, table_name, scraped):
    sqliteConnection = sqlite3.connect(db_name)
    c = sqliteConnection.cursor()

    print("Successfully connected to " + db_name)

    # only create if table is not created yet
    c.execute('''CREATE TABLE IF NOT EXISTS rumah
                 (scrape_timestamp_wib text,
                 title text,
                 bedroom integer,
                 bathroom integer,
                 land_area integer,
                 price integer,
                 kecamatan text,
                 kota_kab text,
                 post_date text,
                 entry_url text,
                 img_url text,
                 rumah_id INTEGER NOT NULL UNIQUE PRIMARY KEY)''')

    print('Start appending to ' + table_name)

    for i in range(len(scraped)):
        count = c.execute('INSERT OR REPLACE INTO ' + table_name + ' VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', list(scraped[i].values()))
    
    sqliteConnection.commit()
    print("Successfully appended data to " + table_name)
    c.close()
    print("Connection closed")

if __name__ == "__main__":

    url = 'https://www.olx.co.id/dijual-rumah-apartemen_c5158'
    db_name = 'new_olx.db'
    table_name = 'rumah'

    content = download_html(url)
    print('Page downloaded')
    scraped = scrape_rumah(content)
    print('Scraping from html tags completed') # output is JSON-ready
    append_rumah_to_sqlite(db_name, table_name, scraped)
    print('Appending completed')
    time.sleep(random.randint(60, 70))