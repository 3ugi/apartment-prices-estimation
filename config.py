BASE_URL = "https://www.otodom.pl"
OFFERS_BASE_URL = f"{BASE_URL}/pl/wyniki/sprzedaz/mieszkanie"
ENTRIES_PER_PAGE = 72
VOIVODESHIP_OFFERS_URL = f"{OFFERS_BASE_URL}/{{voivodeship}}?limit={ENTRIES_PER_PAGE}"

HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'}

VOIVODESHIPS = [
    'dolnoslaskie',
    'kujawsko--pomorskie',
    'lodzkie',
    'lubelskie',
    'lubuskie',
    'malopolskie',
    'mazowieckie',
    'opolskie',
    'podkarpackie',
    'podlaskie',
    'pomorskie',
    'slaskie',
    'swietokrzyskie',
    'warminsko--mazurskie',
    'wielkopolskie',
    'zachodniopomorskie'
]