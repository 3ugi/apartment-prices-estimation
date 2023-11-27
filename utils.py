from unidecode import unidecode

from config import ENTRIES_PER_PAGE, OFFERS_BASE_URL


def process_row(row):
    voivodeship = unidecode(row['voivodeship']).lower()
    powiat = '-'.join(unidecode(row['powiat']).lower().split())
    gmina = '-'.join(unidecode(row['gmina']).lower().split())
    city = '-'.join(unidecode(row['name']).lower().split())
    return OFFERS_BASE_URL + f"/{voivodeship}/{powiat}/{gmina}/{city}" + f"?limit={ENTRIES_PER_PAGE}"


def flatten_list(nested_list):
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))
        else:
            flattened.append(item)
    return flattened
