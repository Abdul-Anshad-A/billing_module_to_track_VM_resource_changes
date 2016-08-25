#!/usr/bin/python
from pysphere import VIServer


class Connect:

	def __init__(self, ip, username, password):
		self.ip = ip
		self.username = username
		self.password = password

	def connectServer(self):
		global server
		server = VIServer()
		#server.connect(self.ip, self.username, self.password, trace_file="debug.txt")
		server.connect(self.ip, self.username, self.password)
		return server

	def disconnectServer(self):
		server.disconnect()
