# RHPy
Sencillo motor para mineria de datos HIVE+PANDAS+R.
Requerimientos de Software
==========================

1.-Consigue Python2.7 and pip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #apt-get install python2.7 pip2

2.-Consigue interprete R
~~~~~~~~~~~~~~~~~~~~~~~~
    #apt-get install r-base r-base-dev r-recommended r-base-core r-base-html

3.-Otra paqueteria y librerias necesarias
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #apt-get install build-essential autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev libsasl2-2 python-setuptools libsasl2-dev python-rpy2 python-rpy2 python-pandas

4.-Instalar y parchar impyla
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #pip2 install impyla
    #tar -xvf impala.tar.xz
    #yes | cp -Rv  impala/* /usr/local/lib/python2.7/dist-packages/impala

5.-Prueba conexion
~~~~~~~~~~~~~~~~~~
    Edita config.py, modificando y agregando los datos de conexion de tu
    Hive.
    ejecuta 
    #python2.7 Testconfig.py 
    o
    #./Testconfig.py

Ejemplos simples de funciones y/o modulos.
==========================================

