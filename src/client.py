from time import sleep
import sys
import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send_info(self, message):
        self.sock.send(message)

    def send_data(self, message):
        self.sock.send(message)
        data = self.sock.recv(1024)
        return data
        #print(data.decode('utf-8'))

