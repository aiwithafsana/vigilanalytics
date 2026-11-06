"""Deterministic canonical identity keys. Empty/unparseable keys stay empty."""
from collections import defaultdict
from functools import lru_cache
import re
import unicodedata

import usaddress

_CREDENTIALS = {'MD', 'DO', 'LLC', 'INC', 'PC', 'PLLC', 'DDS', 'DMD', 'PHD', 'NP', 'RN', 'PA', 'FACP'}
_ABBREVIATIONS = {
    'STREET': 'ST', 'AVENUE': 'AVE', 'ROAD': 'RD', 'BOULEVARD': 'BLVD',
    'DRIVE': 'DR', 'LANE': 'LN', 'COURT': 'CT', 'PLACE': 'PL', 'PARKWAY': 'PKWY',
    'HIGHWAY': 'HWY', 'CIRCLE': 'CIR', 'TERRACE': 'TER', 'TRAIL': 'TRL',
    'SQUARE': 'SQ', 'EXPRESSWAY': 'EXPY', 'TURNPIKE': 'TPKE',
    'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
    'NORTHEAST': 'NE', 'NORTHWEST': 'NW', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW',
}


def clean_text(raw: str | None) -> str:
    text = unicodedata.normalize('NFKC', str(raw or '')).upper()
    return ' '.join(re.sub(r'[^\w\s]', ' ', text, flags=re.UNICODE).replace('_', ' ').split())


def normalize_name(raw: str | None) -> str:
    text = unicodedata.normalize('NFKC', str(raw or '')).upper()
    text = re.sub(r'\b(M|D|P)\.(D|O|C)\.?\b', lambda m: m[1] + m[2], text)
    def strip(value):
        tokens = clean_text(value).split()
        while tokens and tokens[-1] in _CREDENTIALS:
            tokens.pop()
        return ' '.join(tokens)
    if ',' in text:
        parts = [strip(part) for part in text.split(',')]
        # Comma-delimited individual names are surname-first; corporate suffix
        # parts disappear before this rotation (ACME, INC -> ACME).
        text = ' '.join([*parts[1:], parts[0]])
    return strip(text)


def normalize_phone(raw: str | None) -> str:
    text = re.split(r'(?i)\b(?:ext\.?|extension|x)\s*\d+', str(raw or ''))[0]
    digits = re.sub(r'\D', '', text)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits if len(digits) == 10 else ''


def zip5(raw: str | None) -> str:
    match = re.match(r'^\s*(\d{5})(?:[-\s]?\d{4})?\s*$', str(raw or ''))
    return match[1] if match else ''


@lru_cache(maxsize=200_000)
def _parse_address(text: str) -> str:
    if not text.strip():
        return ''
    labels = defaultdict(list)
    for token, label in usaddress.parse(text):
        labels[label].append(token)
    def part(label):
        return clean_text(' '.join(labels[label]))
    number = part('AddressNumber')
    street = ' '.join(part(label) for label in [
        'StreetNamePreDirectional', 'StreetNamePreModifier', 'StreetNamePreType',
        'StreetName', 'StreetNamePostType', 'StreetNamePostDirectional'])
    street = ' '.join(_ABBREVIATIONS.get(token, token) for token in street.split())
    unit = part('OccupancyIdentifier')
    if part('OccupancyType') in {'FLOOR', 'FL'} and unit:
        unit = 'FL ' + unit
    postal = zip5(part('ZipCode'))
    if not number and part('USPSBoxID'):
        number, street = part('USPSBoxID'), 'PO BOX'
    # A conservative empty key prevents unparseable or unlocated addresses
    # from becoming accidental shared-location links. Raw strings survive.
    if not number or not street or not postal:
        return ''
    return f'{number}|{street}|{unit}|{postal}'


def normalize_address(raw_components: dict | str | None) -> str:
    if isinstance(raw_components, dict):
        line1 = raw_components.get('line1', '') or ''
        line2 = raw_components.get('line2', '') or ''
        city = raw_components.get('city', '') or ''
        state = raw_components.get('state', '') or ''
        postal = zip5(raw_components.get('zip', ''))
        if not str(line1).strip() or not postal:
            return ''
        text = f'{line1} {line2}, {city}, {state} {postal}'
    else:
        text = str(raw_components or '')
    text = unicodedata.normalize('NFKC', text).upper()
    text = re.sub(r'\b(?:SUITE|STE)\.?\s*#?\s*', 'STE ', text)
    text = re.sub(r'#\s*', 'STE ', text)
    return _parse_address(' '.join(text.split()))


def normalize_authority(raw: str | None) -> str:
    compact = re.sub(r'[^a-z0-9]', '', str(raw or '').lower())
    match = re.fullmatch(r'1128([ab])(\d+)', compact)
    return f'1128({match[1]})({int(match[2])})' if match else clean_text(raw)


def fraud_related(raw: str | None) -> bool:
    return normalize_authority(raw) in {'1128(a)(1)', '1128(a)(3)', '1128(b)(7)'}
