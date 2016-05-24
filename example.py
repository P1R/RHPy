#!/usr/bin/python2.7
from impala.dbapi import connect
from impala.util import as_pandas
from pandas import *
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
import pandas.rpy.common as com

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
from ofertas_energia_david where tipo_reporte='TE' limit 100
"""
#conecta con hivedoop
conn = connect(host='somehost', port=123, user='someuser', password='mypass')
#crea cursor
cursor = conn.cursor()
#ejecuta Qry
cursor.execute(Qry)
#da formato pandas
df = as_pandas(cursor)
#print df.describe()
#pasando a formato R y a R
rdf = com.convert_to_r_dataframe(df)
#print type(df)
#print type(rdf)
ro.globalenv['tabla'] = rdf
#print ro.r('tabla')
#muestra primer fila de tabla
print ro.r('tabla[1,4:14]')
#calcula el maximo primer fila de tabla
print ro.r('max(tabla[1,4:14])')
cursor.close()
