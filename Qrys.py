from impala.util import as_pandas
from pandas import *
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
import pandas.rpy.common as com

class Checks(object):
    ''' Multiple Predefined Hive SQL Querys'''
    def MaxTE(self, Cursor, Date, Limit=100):
        '''Calcula Maximo de precio en fila
        debe llevar fecha y limite
        '''
        Qry = """
        select id_ofertas_energia_david,
        hora,
        central,
        precio_per_mw_1,
        precio_per_mw_2,
        precio_per_mw_3,
        precio_per_mw_4,
        precio_per_mw_5,
        precio_per_mw_6,
        precio_per_mw_7,
        precio_per_mw_8,
        precio_per_mw_9,
        precio_per_mw_10,
        precio_per_mw_11
        from ofertas_energia_david 
        where tipo_reporte='TE' and fecha_inicial='{0}' limit {1}
        """.format(Date, Limit)
        Cursor.execute(Qry)
        #da formato pandas
        df = as_pandas(Cursor)
        #pasando a formato R y a R
        df = com.convert_to_r_dataframe(df)
        #print type(rdf)
        ro.r('source("./Rfunctions/max.R")')
        ro.globalenv['tabla'] = df
        ro.r('Out <- Rmax(tabla)')
        print ro.r('Out')
        
