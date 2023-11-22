import asyncio
import re
import requests
import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from collections import namedtuple
from typing import Any


BASE_URL = "https://www.otodom.pl"
OFFERS_URL = BASE_URL + "/pl/wyniki/sprzedaz/mieszkanie/opolskie?limit=36&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing"
                                         
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



def get_offers_urls(url: str) -> list[str]:
    parser = BeautifulSoup(requests.get(url, headers=HEADERS).text, 'html.parser')
    offers = parser.find_all('a', attrs={'data-cy':'listing-item-link'})
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


async def get_all_offers_data(urls) -> list[dict]:
    async with ClientSession() as session:
        tasks = [get_offer_data(session, url) for url in urls]
        return await asyncio.gather(*tasks)


async def main():
    offers_urls = get_offers_urls(OFFERS_URL)
    all_offers_data = await get_all_offers_data(offers_urls)
    print(type(all_offers_data))
    for data in all_offers_data:
        print(data)


start_time = time.time()
asyncio.run(main())
print(f'ELAPSED TIME: {time.time() - start_time}')
