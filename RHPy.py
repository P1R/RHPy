#!/usr/bin/python2.7
from ConEct import ConEct
from Qrys import Checks
CE = ConEct()
conn, cursor = CE.db_connect()
CK =  Checks()
CK.MaxTE(cursor,'2016-02-03',200)

