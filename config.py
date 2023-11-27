from collections import namedtuple

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
