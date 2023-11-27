import asyncio
import pandas as pd
import re
import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from more_itertools import chunked
from typing import Any

from config import BASE_URL, FIELDS, HEADERS
from utils import flatten_list, process_row


df = pd.read_csv(
    'populated_places_in_poland.csv', 
    names=['name', 'type', 'gmina', 'powiat', 'voivodeship', 'registry_id', 'suffix', 'adjective'],
    sep=',', low_memory=False, header=0)
df.drop(columns=['registry_id', 'suffix', 'adjective'], inplace=True)
df = df[df['type'] == 'miasto'].reset_index(drop=True)


cities_offers_urls = df.apply(process_row, axis=1)


async def get_offers_pages_urls(session: ClientSession, url: str) -> list[str]:
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            pagination = parser.find('nav', attrs={'data-cy':'pagination'})
            available_pages = pagination.find_all('a', attrs={'class':'eo9qioj1 css-5tvc2l edo3iif1'}) if pagination else None
            last_page_number = max([int(page.text) for page in available_pages]) if available_pages else 1
            return [f"{url}&page={number}" for number in range(1, last_page_number + 1)]


async def get_offers_urls_from_page(session: ClientSession, url: str) -> list[str]:
    url_splitted = url.split('/')
    voivodeship = url_splitted[-4]
    powiat = url_splitted[-3]
    gmina = url_splitted[-2]
    city = url_splitted[-1].split('?')[0]            
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            offers_listing = parser.find('div', attrs={'data-cy':'search.listing.organic'})
            offers = offers_listing.find_all('a', attrs={'data-cy':'listing-item-link'}) if offers_listing else []
    return [(voivodeship, powiat, gmina, city, offer['href']) for offer in offers]


async def get_offer_data(session: ClientSession, offer_url: str) -> dict[str, Any]:
    offer_data = {}
    url = BASE_URL + offer_url
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            for field in FIELDS:
                try:
                    element = parser.find(field.tag, attrs={field.attr_name: field.attr_value})
                    if element:
                        if field.name == 'online_service':
                            offer_data[field.name] = element.find('div', attrs={'class': 'css-1wi2w6s enb64yk5'}).text.strip()
                        elif field.name in ['rooms_num', 'market']:
                            offer_data[field.name] = element.find('a', attrs={'class': 'css-19yhkv9 enb64yk0'}).text.strip()
                        elif field.name == 'address':
                            address = element.text.split(',')
                            is_street = address[0].strip().startswith('ul.') or address[0].strip().startswith('al.') 
                            offer_data['street'] = address[0].strip() if is_street else None
                        elif getattr(field, 'regex'):
                            offer_data[field.name] = re.search(field.regex, element.text).group()
                        else:
                            offer_data[field.name] = element.text.strip()
                    else:
                        offer_data[field.name] = None
                except AttributeError:
                    offer_data[field.name] = None
        return offer_data


async def get_offers_urls_from_all_pages(url: str) -> list[list]:
    async with ClientSession() as session:
        pages_urls = await get_offers_pages_urls(session, url)
        len_pages_urls = len(pages_urls) if pages_urls else 0
        print(url)
        print(len_pages_urls)
        tasks = [get_offers_urls_from_page(session, url) for url in pages_urls] if pages_urls else []
        return await asyncio.gather(*tasks)


async def get_all_offers_urls(urls: list[str]) -> list[list]:
    tasks = [get_offers_urls_from_all_pages(url) for url in urls]
    return await asyncio.gather(*tasks)


async def get_offers_data(urls: list[str]) -> list[dict]:
    async with ClientSession() as session:
        tasks = [get_offer_data(session, url) for url in urls]
        return await asyncio.gather(*tasks)


async def main():
    cities_offers_urls_chunked = chunked(cities_offers_urls, 5)
    for offers_urls_chunk in cities_offers_urls_chunked:
        offers_urls = await get_all_offers_urls(offers_urls_chunk)
        offers_urls_flat = flatten_list(offers_urls)
        print(len(offers_urls_flat))
        filename = f'offers_urls_{time.time()}.txt'
        with open(filename, 'w') as f:
            for url in offers_urls_flat:
                f.write(f'{url}\n')
        print("DELAYING EXECUTION TO LIMIT NUMBER OF REQUESTS...")
        time.sleep(180)
        print("RESUMING EXECUTION...")
    # offers_data = await get_offers_data(offers_urls_flat)
    # print(len(offers_data))


start_time = time.time()
asyncio.run(main())

# filename = 'offers_urls.txt'
# with open(filename, 'r') as f:
#     offers_urls = [line.strip() for line in f]
# print(len(offers_urls))

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
