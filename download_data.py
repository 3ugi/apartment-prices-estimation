import asyncio
import json
import os
import pandas as pd
import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from more_itertools import chunked
from typing import Any

from config import BASE_URL, HEADERS
from utils import flatten_list, process_row


df = pd.read_csv(
    'populated_places_in_poland.csv', 
    names=['name', 'type', 'gmina', 'powiat', 'voivodeship', 'registry_id', 'suffix', 'adjective'],
    sep=',', low_memory=False, header=0)
df.drop(columns=['registry_id', 'suffix', 'adjective'], inplace=True)
df = df[df['type'] == 'miasto'].reset_index(drop=True)


cities_offers_urls = df.apply(process_row, axis=1)

voivodeships_offers_urls = [
    'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/opolskie?limit=72'
]


async def get_offers_pages_urls(session: ClientSession, url: str) -> list[str]:
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            pagination = parser.find('nav', attrs={'data-cy':'pagination'})
            # available_pages = pagination.find_all('a', attrs={'class':'eo9qioj1 css-5tvc2l edo3iif1'}) if pagination else None
            available_pages = pagination.find_all('a', attrs={'class':'eo9qioj1 css-pn5qf0 edo3iif1'}) if pagination else None
            last_page_number = max([int(page.text) for page in available_pages]) if available_pages else 1
            return [f"{url}&page={number}" for number in range(1, last_page_number + 1)]


async def get_offers_urls_from_page(session: ClientSession, url: str) -> list[str]: 
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            offers_listing = parser.find('div', attrs={'data-cy':'search.listing.organic'})
            offers = offers_listing.find_all('a', attrs={'data-cy':'listing-item-link'}) if offers_listing else []
    try:
        return [offer['href'] for offer in offers]
    except UnboundLocalError:
        print(f"ERROR: Could not get offers for {url}")


async def get_offer_data(session: ClientSession, offer_url: str) -> dict[str, Any]:
    offer_data = {}
    url = BASE_URL + offer_url
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            script_tag = parser.find('script', attrs={'id': '__NEXT_DATA__'})
            if script_tag:
                replacements = {':false': ':False', ':true': ':True', ':null': ':None'}
                script_tag_text = script_tag.text.strip()
                for old, new in replacements.items():
                    script_tag_text = script_tag_text.replace(old, new)
                script_tag_dict = eval(script_tag_text)

                address = script_tag_dict['props']['pageProps']['ad']['location']['address']
                address = address['street']['name'] if address['street'] else None
                offer_data.update(address=address)

                properties = {
                    **script_tag_dict['props']['pageProps']['ad']['property']['properties'],
                    **script_tag_dict['props']['pageProps']['ad']['property']['buildingProperties']
                    }
                del properties['__typename']
                for key in properties.keys():
                    value = properties[key]
                    value = value if not isinstance(value, list) else ', '.join(value) or None
                    offer_data[key] = value

                ad_properties = script_tag_dict['props']['pageProps']['ad']['property']
                offer_data.update(condition=ad_properties['condition'])
                offer_data.update(ownership=ad_properties['ownership'])

                rent = ad_properties['rent']
                rent, rent_currency = (rent['value'], rent['currency']) if rent else (None, None)
                offer_data.update(rent=rent)
                offer_data.update(rent_currency=rent_currency)

                ad_data_keys = (
                    'lat', 'long', 'ad_price', 'price_currency', 'city_name', 'market', 'region_name', 'subregion_id',
                    'Area', 'Media_types', 'ProperType', 'user_type', 'OfferType'
                )
                ad_data = {
                    **script_tag_dict['props']['pageProps']['adTrackingData'],
                    **script_tag_dict['props']['pageProps']['ad']['target']
                }
                for key in ad_data_keys:
                    value = ad_data.get(key, None)
                    value = value if not isinstance(value, list) else ', '.join(value)
                    offer_data[key.lower()] = value

                additional_information = script_tag_dict['props']['pageProps']['ad']['additionalInformation']
                add_info_labels = ('free_from', 'lift')
                for dict_elem in additional_information:
                    if dict_elem['label'] in add_info_labels:
                        offer_data[dict_elem['label']] = ', '.join(dict_elem['values']).strip('::') or None

        return offer_data


async def get_offers_urls_from_all_pages(url: str) -> list[list]:
    async with ClientSession() as session:
        pages_urls = await get_offers_pages_urls(session, url)
        len_pages_urls = len(pages_urls) if pages_urls else 0
        print(url)
        print(f"Number of pages: {len_pages_urls}")
        tasks = [get_offers_urls_from_page(session, url) for url in pages_urls] if pages_urls else []
        return await asyncio.gather(*tasks)


async def get_all_offers_urls(urls: list[str]) -> list[list]:
    tasks = [get_offers_urls_from_all_pages(url) for url in urls]
    return await asyncio.gather(*tasks)


async def get_offers_data(urls: list[str]) -> list[dict]:
    async with ClientSession() as session:
        tasks = [get_offer_data(session, url) for url in urls]
        return await asyncio.gather(*tasks)

# start_time = time.time()
# import requests
# for url in cities_offers_urls:
#     response = requests.get(url, headers=HEADERS)
#     if response.status_code == 404:
#         print(404)
#         with open('404.txt', 'a') as f:
#             f.write(f'{url}\n')
#     elif response.status_code == 403:
#         print(403)
#         with open('403.txt', 'a') as f:
#             f.write(f'{url}\n')
#     elif response.status_code == 200:
#         print(200)
    

async def main():
    delay = 120
    chunk_size = 10
    # cities_offers_urls_chunked = chunked(cities_offers_urls, chunk_size)
    cities_offers_urls_chunked = chunked(voivodeships_offers_urls, chunk_size)
    for offers_urls_chunk in cities_offers_urls_chunked:
        offers_urls = await get_all_offers_urls(offers_urls_chunk)
        offers_urls_flat = flatten_list(offers_urls)
        print(f'Number of offers in {chunk_size}-element chunk: {len(offers_urls_flat)}')
        filename = f'offers_urls_{time.time()}.txt'
        with open(filename, 'w') as f:
            for url in offers_urls_flat:
                f.write(f'{url}\n')
        print(f"DELAYING EXECUTION BY {delay} SECONDS TO LIMIT NUMBER OF REQUESTS...")
        # time.sleep(delay)
        print("RESUMING EXECUTION...")
        break

    # offers_urls_files = [file for file in os.listdir(os.curdir) if file.startswith('offers_urls')]
    # for filename in offers_urls_files:
    #     with open(filename, 'r') as f:
    #         offers_urls = [line.strip() for line in f]
    #     offers_data = await get_offers_data(offers_urls)
    #     with open('offers_data.txt', 'a') as f:
    #         for data in offers_data:
    #             f.write(f'{json.dumps(data)}\n')
    #     print("DELAYING EXECUTION TO LIMIT NUMBER OF REQUESTS...")
    #     time.sleep(delay)
    #     print("RESUMING EXECUTION...")


start_time = time.time()
asyncio.run(main())

elapsed_time_sec = time.time() - start_time
print(f'ELAPSED TIME [SEC]: {round(elapsed_time_sec)}')
print(f'ELAPSED TIME [MIN]: {round(elapsed_time_sec / 60, 1)}')

"""
OFFERS URLSFLAT: 1247
OFFERS DATA: 1247
ELAPSED TIME: 47.78603482246399

7308
7308
ELAPSED TIME: 243.62670183181763
"""
