import unidecode


def name_normalizer(srs): return srs.str.lower().str.strip().str.replace(
    '  ', ' ').fillna('').map(unidecode.unidecode)