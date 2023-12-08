from unidecode import unidecode

from config import ENTRIES_PER_PAGE, OFFERS_BASE_URL


CITIES_WITH_POWIAT_RIGTHS = [
    'biala-podlaska', 'chelm', 'czarnkow', 'elblag', 'golub-dobrzyn', 'gorlice', 
    'gorowo-ilaweckie', 'grudziadz', 'lomza', 'przemysl', 'siedlce', 'skierniewice', 
    'slupsk', 'suwalki', 'tarnow', 'wabrzezno', 'wloclawek', 'zamosc'
]


def process_row(row):
    city = '-'.join(unidecode(row['name']).lower().split())
    gmina = '-'.join(unidecode(row['gmina']).lower().split())
    if gmina.endswith('-m'):
        gmina = gmina[:-2] if city in CITIES_WITH_POWIAT_RIGTHS else f"gmina-miejska--{gmina[:-2]}"
    powiat = '-'.join(unidecode(row['powiat']).lower().split())
    voivodeship = unidecode(row['voivodeship']).lower()
    voivodeship = voivodeship if '-' not in voivodeship else voivodeship.replace('-', '--')
    return OFFERS_BASE_URL + f"/{voivodeship}/{powiat}/{gmina}/{city}" + f"?limit={ENTRIES_PER_PAGE}"


def flatten_list(nested_list):
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))
        else:
            flattened.append(item)
    return flattened
