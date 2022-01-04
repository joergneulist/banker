from babel.numbers import parse_decimal
import csv
import dateutil.parser


def _skip_empty(reader):
    assert(len(next(reader)) == 0)
    
    
def _skip_to(reader, string):
    row = next(reader)
    while not len(row) or not row[0].startswith(string):
        row = next(reader)
    return row


def _parse_number(string, locale = 'de'):
    return float(parse_decimal(string, locale = locale))


def _parse_date(string):
    return dateutil.parser.parse(string)


def _parse(names, types, row):
    dict_row = {}
    for key, dtype, value in zip(names, types, row):
        if key is None:
            continue
        if dtype == 'date':
            value = _parse_date(value)
        elif dtype is not None:
            value = _parse_number(value, locale = dtype)
        
        dict_row[key] = value
    return dict_row



def _read_records(account, fields, dtypes, reader):
    account['fields'] = [field for field in fields if field]
    account['entries'] = [_parse(fields, dtypes, row) for row in reader]
    

def _read_Giro(reader, intro):
    account = {'code': intro.split('/')[0].strip().replace('*', 'x'), 'type': 'Girokonto'}

    row = _skip_to(reader, 'Kontostand')
    account['balance'] = _parse_number(row[1][:-4])
    account['updated'] = _parse_date(row[0][15:-1])
    _skip_empty(reader)
    next(reader)
    _read_records(account,
                  [None, 'Wertstellung', 'Buchungstext', 'Auftraggeber', 'Verwendungszweck', 'Kontonummer', 'BLZ', 'Betrag'],
                  [None, 'date',         None,           None,           None,               None,          None,  'de'],
                  reader)
   
    return account


def _read_VISA(reader, intro):
    account = {'code': intro.replace('*', 'x'), 'type': 'Kreditkarte'}

    account['balance'] = _parse_number(_skip_to(reader, 'Saldo')[1][:-4], locale = 'en')
    account['updated'] = _parse_date(_skip_to(reader, 'Datum')[1])  
    _skip_empty(reader)
    next(reader)
    _read_records(account,
                  [None, 'Wertstellung', 'Belegdatum', 'Beschreibung', 'Betrag'],
                  [None, 'date',         'date',       None,           'de'],
                  reader)
    
    return account


def read_from_file(filename):
    parsers = {
        'Konto': _read_Giro,
        'Kredit': _read_VISA
    }
    with open(filename, 'r', encoding='Windows-1252') as file:
        reader = csv.reader(file, delimiter=';')
        row = next(reader)
        for tag, parser in parsers.items():
            if row[0].startswith(tag):
                return parser(reader, row[1])
    
    raise RuntimeError(f'CSV file type {row[0]} is not supported!')
