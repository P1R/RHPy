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
    def MaxMonth(self, Cursor, Month, Units, Limit=1):
        '''get maximum of month by units'''
        File=open('MaxMonth.csv','w')
        File.write("unidad, fecha, hora, costo_maximo\n")
        for item in Units:
            Qry = """
            select * from(
            select unidad,
            fecha_inicial,
            hora,precio_per_mw_10,
            max(precio_per_mw_10) over
            (partition by unidad order by fecha_inicial,
            hora asc rows between unbounded preceding and unbounded following )
            as costo_maximo from ofertas_energia_david
            where (tipo_reporte='TE' and unidad='{1}' 
            and month(fecha_inicial)={0} and precio_per_mw_10 > 0)
            ) data where data.costo_maximo=precio_per_mw_10 
            and data.costo_maximo > 0 limit {2}
            """.format(Month, item, Limit)
            Cursor.execute(Qry)
            #da formato pandas
            df = as_pandas(Cursor)
            print df
            try:
                File.write("{0}, {1}, {2}, {3}\n".format(str(df['data.unidad'][0]),
                    str(df['data.fecha_inicial'][0]),
                    str(df['data.hora'][0]),
                    str(df['data.costo_maximo'][0])))
            except IndexError:
                print item + " vacio"
                pass
        File.close()

    def AvgMonth(self, Cursor, Month, Units, Limit=1):
        '''GetAverage of month by units'''
        File=open('AvgMonth.csv', 'w')
        File.write("Unidad, promedio\n")
        for item in Units:
            Qry = """
            select avg(precio_per_mw_10) 
            from ofertas_energia_david
            where tipo_reporte='TE' and unidad='{1}' 
            and month(fecha_inicial)={0} and precio_per_mw_10 > 0
            limit {2}
            """.format(Month, item,Limit)
            Cursor.execute(Qry)
            #da formato pandas
            df = as_pandas(Cursor)
            File.write("{0}, {1}\n".format(item,str(df['c0'][0])))
        File.close()
