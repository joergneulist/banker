import json

from frontend_mariadb import database


class db_driver:
    ACC_TABLE = 'Accounts'
    TABLE_COL = 'Tabelle'
    
    def __init__(self, filename: str):
        with open(filename, 'r') as file:
            full_config = json.load(file)
        assert(full_config['driver'] == 'mariadb')
        self.config = full_config['config']

    def _open(self):
        return database(**self.config)

    def _acc_list(self, db):
        return db.read_table(self.ACC_TABLE)
    
    def _acc_load(self, db, table):
        return db.read_table(table)

    def load(self):
        with self._open() as db:
            data = self._acc_list(db)
            for account in data:
                account['Data'] = self._acc_load(db, account[self.TABLE_COL])
        return data
