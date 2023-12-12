import asyncio
import json
import time

from aiohttp import ClientSession
from asyncio_throttle import Throttler
from bs4 import BeautifulSoup
from math import ceil
from more_itertools import chunked
from typing import Any
from unidecode import unidecode

from config import BASE_URL, HEADERS, VOIVODESHIPS, VOIVODESHIP_OFFERS_URL
from utils import calculate_delay, flatten_list


throttler = Throttler(rate_limit=10, period=1)


async def get_offers_pages_urls(session: ClientSession, url: str) -> list[str]:
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            parser = BeautifulSoup(await response.text(), 'html.parser')
            pagination = parser.find('nav', attrs={'data-cy':'pagination'})
            available_pages = pagination.find_all('a', attrs={'class':'eo9qioj1 css-pn5qf0 edo3iif1'}) if pagination else None
            last_page_number = max([int(page.text) for page in available_pages]) if available_pages else 1
            return [f"{url}&page={number}" for number in range(1, last_page_number + 1)]


async def get_offers_urls_from_page(session: ClientSession, url: str) -> list[str]:
    async with throttler:
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    parser = BeautifulSoup(await response.text(), 'html.parser')
                    offers_listing = parser.find('div', attrs={'data-cy':'search.listing.organic'})
                    offers = offers_listing.find_all('a', attrs={'data-cy':'listing-item-link'}) if offers_listing else []
                    return [offer['href'] for offer in offers]
                else:
                    return f"ERROR {response.status}: {url}"
        except Exception as exc:
            return {'error': type(exc).__name__, 'url': url}


async def get_offers_urls_from_all_pages(url: str) -> list[list]:
    async with ClientSession() as session:
        pages_urls = await get_offers_pages_urls(session, url)
        tasks = [get_offers_urls_from_page(session, url) for url in pages_urls]
        return await asyncio.gather(*tasks)


async def get_offer_data(session: ClientSession, offer_url: str) -> dict[str, Any]:
    offer_data = {}
    url = BASE_URL + offer_url
    async with throttler:
        try:
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
                        street = unidecode(address['street']['name']) if address['street'] else None
                        offer_data.update(street=street)

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
                else:
                    return {'error': response.status, 'url': url}
        except Exception as exc:
            return {'error': type(exc).__name__, 'url': url}


async def get_offers_data(urls: list[str]) -> list[dict]:
    async with ClientSession() as session:
        tasks = [get_offer_data(session, url) for url in urls]
        return await asyncio.gather(*tasks)


async def main():
    
    for voivodeship in VOIVODESHIPS:
        start_time = time.time()
        voivodeship_offers_url = VOIVODESHIP_OFFERS_URL.format(voivodeship=voivodeship)
        offers_urls = await get_offers_urls_from_all_pages(voivodeship_offers_url)
        offers_urls_flat = flatten_list(offers_urls)
        print(f'Number of offers in {voivodeship} voivodeship: {len(offers_urls_flat)}')
        filename = f'offers_urls_{voivodeship}.txt'
        with open(filename, 'w') as f:
            for url in offers_urls_flat:
                f.write(f'{url}\n')
        elapsed_time = round(time.time() - start_time)
        delay = calculate_delay(elapsed_time)
        print(f'{voivodeship.upper()} ELAPSED TIME [SEC]: {elapsed_time}')
        print(f"DELAYING EXECUTION BY {delay} SECONDS TO LIMIT NUMBER OF REQUESTS...")
        await asyncio.sleep(delay)
        print("RESUMING EXECUTION")

    chunk_size = 300
    for voivodeship in VOIVODESHIPS:
        print(voivodeship.upper())
        filename = f'offers_urls_{voivodeship}.txt'
        with open(filename, 'r') as f:
            offers_urls = [line.strip() for line in f]
        offers_urls_chunked = chunked(offers_urls, chunk_size)
        chunk_id = 1
        for offers_urls_chunk in offers_urls_chunked:
            print(f'PROCESSING CHUNK: {chunk_id}/{ceil(len(offers_urls)/chunk_size)}, SIZE: {chunk_size}...')
            start_time = time.time()
            offers_data = await get_offers_data(offers_urls_chunk)
            filename = f'offers_data_{voivodeship}_{chunk_id}.txt'
            with open(filename, 'w') as f:
                for data in offers_data:
                    f.write(f'{json.dumps(data)}\n')
            elapsed_time = round(time.time() - start_time)
            delay = calculate_delay(elapsed_time)
            print(f'CHUNK {chunk_id} ELAPSED TIME [SEC]: {elapsed_time}\n')
            print(f"DELAYING EXECUTION BY {delay} SECONDS TO LIMIT NUMBER OF REQUESTS...")
            await asyncio.sleep(delay)
            print("RESUMING EXECUTION\n")
            chunk_id += 1


start_time = time.time()
asyncio.run(main())
elapsed_time_sec = time.time() - start_time
print(f'ELAPSED TIME [SEC]: {round(elapsed_time_sec)}')
print(f'ELAPSED TIME [MIN]: {round(elapsed_time_sec / 60, 1)}')
