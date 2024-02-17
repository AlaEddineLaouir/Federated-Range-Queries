import psycopg2

class Connection:
    
    def __init__(self,sql_db):
        # Adjust the user and password  as per your configuration.
        self.conn =psycopg2.connect(database=sql_db, user='alaeddinelaouir', password='', host='127.0.0.1', port= '5432')

    
    def __del__(self):
        self.conn.commit()
        self.conn.close()



