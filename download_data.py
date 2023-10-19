import requests

from bs4 import BeautifulSoup


URL = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/opolskie?limit=36&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing"
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'}


def get_offers_urls():
    parser = BeautifulSoup(requests.get(URL, headers=HEADERS).text, 'html.parser')
    offers = parser.find_all('a', attrs={'data-cy':'listing-item-link'})
    return [offer['href'] for offer in offers]


offers_urls = get_offers_urls()
print(len(offers_urls))
print(offers_urls)
