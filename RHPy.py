#!/usr/bin/python2.7
from ConEct import ConEct
from Qrys import Checks
from ListaUnidades import Units as Unt
CE = ConEct()
conn, cursor = CE.db_connect()
CK =  Checks()
#CK.MaxTE(cursor,'2016-02-03',200)
#CK.AvgMonth(cursor,2,Unt)
CK.MaxMonth(cursor,2,Unt)
