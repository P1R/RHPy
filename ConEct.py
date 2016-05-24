from config import DbData as DbD
from impala.dbapi import connect

class ConEct(object):
    '''Manejo de conexiones Hadoop con Impyla'''

    def __init__(self):
        self.ConnectionError ="Error de conexion, porfavor verifica datos\
                en config.py"

    def db_connect(self):
        '''Funcion que conecta a hive con datos config.py regresando
        conector y cursor'''

        conn = connect(host=DbD['host'],
                port=DbD['port'],
                user=DbD['user'],
                password=DbD['password'])
        cursor = conn.cursor()
        return conn, cursor

    def db_close(self, conn):
        '''desconecta cursor y base de datos'''
        conn.close() 

if __name__ == "__main__":
    pass

