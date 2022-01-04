import sys

from db_driver import db_driver
import import_file_dkb


def load(db):
    data = db.load()
    for acc in data:
        acc['Latest'] = max([x['Wertstellung'] for x in acc['Data']])
    
    return data


def overview(data):
    for acc in data:
        print('account "{}" ({}) has {} records, the latest from {}'\
              .format(acc['Name'], acc['Code'], len(acc['Data']), acc['Latest']))


def find_account(data, code):
    lookup = {acc['Code']: idx for idx, acc in enumerate(data)}
    if code in lookup:
        return data[lookup[code]]       


def update(db, data, filename):
    print(f'READ: {filename}')
    file_data = import_file_dkb.read_from_file(filename)
    print('FOUND: account {} has {} records'.format(file_data['code'], len(file_data['entries'])))

    acc = find_account(data, file_data['code'])
    print('DATABASE: account {} has {} records, the latest from {}'.format(acc['Name'], len(acc['Data']), acc['Latest']))

    inserted = 0
    #for entry in account['entries'][::-1]:
    #    if not newest or entry['Wertstellung'].date() > newest:
    #        db.insert(account['id'], account['fields'], entry)
    #        inserted += 1
    #    else:
    #        if db.insert_unique(account['id'], account['fields'], entry):
    #            inserted += 1      

    #db.commit()
    print(f'STORE: {inserted} new records')


if __name__ == "__main__":
    db = db_driver('db_config.json')
    data = load(db)
    overview(data)

    if len(sys.argv) > 1:
        update(db, data, sys.argv[1])
