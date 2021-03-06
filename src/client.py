from time import sleep
import sys
import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send_data(self, message):
        print('Sending request to server ',message)
        self.sock.send(message.encode())
        data = self.sock.recv(1024)
        print('Response from server ',data.decode())
        return data.decode()
        #print(data.decode('utf-8'))

