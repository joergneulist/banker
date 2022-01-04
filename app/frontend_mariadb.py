import mariadb

class database:

    @staticmethod                                                               
    def make_select(table, fields, qualifier = ''):
        return ('SELECT {} FROM {} {}'
                .format(','.join(fields), table, qualifier))
    
    @staticmethod                                                               
    def make_select_match(table, fields, schema):
        schema_query = [f'{name} = %({name})s' for name in schema]
        return ('SELECT {} FROM {} WHERE {}'
                .format(','.join(fields), table, ' AND '.join(schema_query)))
    
    @staticmethod                                                               
    def make_insert(table, schema):
        return ('INSERT INTO {} ({}) VALUES ({})'
                .format(table, ','.join(schema), ','.join(['%({})s'.format(name) for name in schema])))

    
    def __init__(self, host, db, user, password):
        self.cnx = mariadb.connect(host = host, db = db, user = user, password = password)
        self.cursor = self.cnx.cursor()
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def raw_query(self, *query_data):
        #print(query_data)
        self.cursor.execute(*query_data)
        
        result = []
        if self.cursor.description:
            for row in self.cursor:
                result.append(row)
        
        return result
    
    def insert(self, table, schema, data):
        _ = self.raw_query(self.make_insert(table, schema), data)
    
    def insert_unique(self, table, schema, data):
        result = self.select_match(table, ['COUNT(*)'], schema, data)
        if result[0][0] == 0:
            _ = self.raw_query(self.make_insert(table, schema), data)
            return True
        return False
    
    def columns(self, table):
        return self.raw_query(f"SHOW COLUMNS FROM {table}")
    
    def select(self, table, fields, qualifier = ''):
        return self.raw_query(self.make_select(table, fields, qualifier))
    
    def select_match(self, table, fields, schema, data):
        return self.raw_query(self.make_select_match(table, fields, schema), data)
    
    def read_table(self, table, qualifier = ''):
        headers = [row[0] for row in self.columns(table)]
        data = []
        for row in self.select(table, ['*'], qualifier):
             data.append({key: value for key, value in zip(headers, row)})
        return data
    
    def commit(self):
        self.cnx.commit()
    
    def close(self):
        self.cursor.close()
        self.cnx.close()
