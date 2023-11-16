import re
import requests

from bs4 import BeautifulSoup
from typing import Any


BASE_URL = "https://www.otodom.pl"
OFFERS_URL = BASE_URL + "/pl/wyniki/sprzedaz/mieszkanie/opolskie?limit=36&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing"
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'}


def get_offers_urls() -> list[str]:
    parser = BeautifulSoup(requests.get(OFFERS_URL, headers=HEADERS).text, 'html.parser')
    offers = parser.find_all('a', attrs={'data-cy':'listing-item-link'})
    return [offer['href'] for offer in offers]


def get_offer_data(offer_url: str) -> dict[str, Any]:
    offer_data = {}
    url = BASE_URL + offer_url
    parser = BeautifulSoup(requests.get(url, headers=HEADERS).text, 'html.parser')
    area = parser.find('div', attrs={'data-testid':'table-value-area'}).text
    rooms_num = parser.find('a', attrs={'class':'css-19yhkv9 enb64yk0'}).text
    floor = parser.find('div', attrs={'data-testid':'table-value-floor'}).text

    offer_data.update(area=re.search(r'\d+[,|.]?\d*', area)[0])
    offer_data.update(rooms_num=rooms_num)
    offer_data.update(floor=floor)
    print(offer_data)


print(OFFERS_URL, '')
offers_urls = get_offers_urls()
print(len(offers_urls))

print(BASE_URL + offers_urls[0])
get_offer_data(offers_urls[0])
