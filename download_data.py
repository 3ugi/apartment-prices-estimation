import asyncio
import pandas as pd
import re
import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from collections import namedtuple
from unidecode import unidecode
from typing import Any


BASE_URL = "https://www.otodom.pl"
OFFERS_BASE_URL = f"{BASE_URL}/pl/wyniki/sprzedaz/mieszkanie"
ENTRIES_PER_PAGE = 72

HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'}

DECIMAL_NUMBER_REGEX = r'\d+[,|.]?\d*'
Field = namedtuple('Field', ['name', 'tag', 'attr_name', 'attr_value', 'regex'])

FIELDS = [
    Field('area', 'div', 'data-testid', 'table-value-area', DECIMAL_NUMBER_REGEX),
    Field('rooms_num', 'div', 'data-testid', 'table-value-rooms_num', None),
    Field('floor', 'div', 'data-testid', 'table-value-floor', None),
    Field('rent', 'div', 'data-testid', 'table-value-rent', DECIMAL_NUMBER_REGEX),
    Field('online_service', 'div', 'aria-label', 'Obsługa zdalna', None),
    Field('building_ownership', 'div', 'data-testid', 'table-value-building_ownership', None),
    Field('construction_status', 'div', 'data-testid', 'table-value-construction_status', None),
    Field('outdoor', 'div', 'data-testid', 'table-value-outdoor', None),
    Field('car_park', 'div', 'data-testid', 'table-value-car', None),
    Field('heating', 'div', 'data-testid', 'table-value-heating', None),

    Field('market', 'div', 'data-testid', 'table-value-market', None),
    Field('advertiser_type', 'div', 'data-testid', 'table-value-advertiser_type', None),
    Field('free_from', 'div', 'data-testid', 'table-value-free_from', None),
    Field('build_year', 'div', 'data-testid', 'table-value-build_year', None),
    Field('building_type', 'div', 'data-testid', 'table-value-building_type', None),
    Field('windows_type', 'div', 'data-testid', 'table-value-windows_type', None),
    Field('media_types', 'div', 'data-testid', 'table-value-media_types', None),
    Field('security_types', 'div', 'data-testid', 'table-value-security_types', None),
    Field('equipment_types', 'div', 'data-testid', 'table-value-equipment_types', None),
    Field('building_material', 'div', 'data-testid', 'table-value-building_material', None),

    Field('address', 'a', 'aria-label', 'Adres', None),
]


df = pd.read_csv(
    'populated_places_in_poland.csv', 
    names=['name', 'type', 'gmina', 'powiat', 'voivodeship', 'registry_id', 'suffix', 'adjective'],
    sep=',', low_memory=False, header=0)
df.drop(columns=['registry_id', 'suffix', 'adjective'], inplace=True)
df = df[df['type'] == 'miasto'].reset_index(drop=True)


def _process_row(row):
    voivodeship = unidecode(row['voivodeship']).lower()
    powiat = '-'.join(unidecode(row['powiat']).lower().split())
    gmina = '-'.join(unidecode(row['gmina']).lower().split())
    city = '-'.join(unidecode(row['name']).lower().split())
    return OFFERS_BASE_URL + f"/{voivodeship}/{powiat}/{gmina}/{city}" + f"?limit={ENTRIES_PER_PAGE}"


cities_offers_urls = df.apply(_process_row, axis=1)


async def get_offers_pages_urls(session: ClientSession, url: str) -> list[str]:
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            pagination = parser.find('nav', attrs={'data-cy':'pagination'})
            available_pages = pagination.find_all('a', attrs={'class':'eo9qioj1 css-5tvc2l edo3iif1'}) if pagination else None
            last_page_number = max([int(page.text) for page in available_pages]) if available_pages else 1
            return [f"{url}&page={number}" for number in range(1, last_page_number + 1)]


async def get_offers_urls_from_page(session: ClientSession, url: str) -> list[str]:            
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            offers_listing = parser.find('div', attrs={'data-cy':'search.listing.organic'})
            offers = offers_listing.find_all('a', attrs={'data-cy':'listing-item-link'}) if offers_listing else []
    return [offer['href'] for offer in offers]


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
                            offer_data['street'] = address[0].strip() if address[0].strip().startswith('ul.') else None
                        elif getattr(field, 'regex'):
                            offer_data[field.name] = re.search(field.regex, element.text).group()
                        else:
                            offer_data[field.name] = element.text.strip()
                    else:
                        offer_data[field.name] = None
                except AttributeError:
                    offer_data[field.name] = None
        return offer_data


async def get_offers_urls_from_all_pages(url: str) -> list[dict]:
    async with ClientSession() as session:
        pages_urls = await get_offers_pages_urls(session, url)
        len_pages_urls = len(pages_urls) if pages_urls else 0
        print(len_pages_urls)
        tasks = [get_offers_urls_from_page(session, url) for url in pages_urls] if pages_urls else []
        return await asyncio.gather(*tasks)


async def get_offers_data(urls: list[str]) -> list[dict]:
    async with ClientSession() as session:
        tasks = [get_offer_data(session, url) for url in urls]
        return await asyncio.gather(*tasks)

# cities_offers_urls = [
#     "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/lodzkie/zgierski/aleksandrow-lodzki/aleksandrow-lodzki?limit=72"
# ]
async def main():
    for city_offers_url in cities_offers_urls:
        print(city_offers_url)
        offers_urls = await get_offers_urls_from_all_pages(city_offers_url)
        offers_urls_flat = [url for urls in offers_urls for url in urls]
        offers_data = await get_offers_data(offers_urls_flat)
        print(len(offers_urls_flat))
        print(len(offers_data))


start_time = time.time()
asyncio.run(main())
print(f'ELAPSED TIME: {time.time() - start_time}')

"""
PAGES URLS: 20
OFFERS URLSFLAT: 1247
OFFERS DATA: 1247
ELAPSED TIME: 47.78603482246399
"""
