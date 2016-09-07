#!/usr/bin/python
# -*- coding: utf-8 -*-
# -*- decoding: utf-8 -*-
import socket
import thread
import threading
import pyRserve

def sql_query(holder, sentence):
   sql2='{0} <- dbGetQuery(conn,"{1}")' .format (holder,sentence)
   return sql2

def consultar_hive(consulta) :
	conn = pyRserve.connect(host='10.71.1.30', port=6311)
	conn.atomicArray = True
	R_Loadder = """library(rJava)
	library(RJDBC)
	.jinit()
	#Sys.setenv(HADOOP_JAR= paste0("", collapse=.Platform$path.sep))
	driverclass = "org.apache.hive.jdbc.HiveDriver"
	classPath = c("/usr/lib/hive/apache-hive-1.2.1-bin/lib/hive-jdbc-1.2.1-standalone.jar", "/etc/hadoop-2.7.1/share/hadoop/common/lib/commons-configuration-1.6.jar", "/etc/hadoop-2.7.1/share/hadoop/common/hadoop-common-2.7.1.jar")
	dr2 = JDBC(driverclass,classPath, identifier.quote = "`")
	Sys.setenv(HADOOP_JAR= paste0(classPath, collapse=.Platform$path.sep))
	url = paste0("jdbc:hive2://", "10.71.1.30", ":", "10000", "/default",";auth=noSasl")
	dbConnect(dr2, url) -> conn"""
	# Abrimos conexión
	print "...Conectando..."
	conn.eval(R_Loadder)
	print "...Consultando..."
	conn.eval(sql_query('hive_rqt', consulta))
	print conn.eval('hive_rqt')
	respuesta = conn.eval('hive_rqt')
	return str(respuesta)

def atiende_cliente(sock_client, sock_ip) :
	print "{0}.0 - Lanzando el hilo para atender al cliente... " + str(sock_client)
	# Recinimos los datos del cliente
	control = 0
	i = 0;

	while control != 1 :
		''' Control puede tomar diferentes valores, 
		dependiendo de lo que reciba por parte del cliente.
		0 - Inicio del canal de comunicación
		1 - Cerrar el canal de comunicación
		2 - Recibir cadena SQL
		'''
		try:
			print "{0}.{1} - Esperando mensaje de {2}... ".format(sock_ip, i, sock_ip)
			mensaje = sock_client.recv(1000)
			respuesta = ""

			i = i + 1
			print "{0}.{1} - Cliente {2} dice: {3}".format(sock_ip, i, sock_ip, mensaje)

			if len(str(mensaje)) == 0 or str(mensaje) == "" :
				# No tengo ni idea
				sock_client.close()
				i = i + 1
				print "{0}.{1} - Conexión cerrada...".format(sock_ip, i)
				control = 1
			else :
				if mensaje.isdigit() :
					# Si el primer mensaje es válido, comenzamos la espera por los mensajes subsecuentes
					control = int(mensaje)

					i = i + 1
					print "{0}.{1} - Código de control: {2}".format(sock_ip, i, control)

					if control == 1 :
						i = i + 1
						print "{0}.{1} - Cerrando conexión con {2}... ".format(sock_ip, i, sock_ip)

						# Cerramos la conexión.format(sock_ip, i
						sock_client.close()
						i = i + 1
						print "{0}.{1} - Adios!!! ".format(sock_ip, i)
					elif control == 2 :
						i = i + 1
						print "{0}.{1} - Ejecución de SQL... ".format(sock_ip, i)
						# Recibimos cadena SQL
						mensaje = str(sock_client.recv(5000))
						i = i + 1
						print "{0}.{1} - Cliente {2} dice: {3}".format(sock_ip, i, sock_ip, mensaje)
						# Hacemos la petición a Haddop
						respuesta = consultar_hive(mensaje)
						i = i + 1
						print "{0}.{1} - Hive dice: {2}".format(sock_ip, i, respuesta)
						i = i + 1
						print "{0}.{1} - Le enviamos al cliente {2}:".format(sock_ip, i, respuesta)
						# Le respondemos al cliente
						sock_client.send(respuesta)
						# Fijamos el control en 0
						control = 0
					else :
						# No definido qué hacer
						control = 0
				else :
					control = 0
		except Exception, e:
			sock_client.close()
			i = i + 1
			print "{0} - Conexión abortada por el error: {1} ".format(i, e)
			break


def server_socket(ip, port) :
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server.bind((ip, port))
	server.listen(10)

	# bucle para atender clientes
	while 1 :
		print "SERVER: Esperando..."
		# Se espera a un cliente
		socket_cliente, datos_cliente = server.accept()
		# Se escribe su informacion
		print "SERVER: Cliente aceptado: " + str(datos_cliente)
		thread.start_new_thread(atiende_cliente, (socket_cliente, datos_cliente))


if __name__ == "__main__":
	#print "Hola!!!!!"
	server_socket("", 9580)
