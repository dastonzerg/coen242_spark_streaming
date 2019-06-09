import socket
import sys

sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_address = ('localhost', 9999)
sock_udp.bind(udp_address)

sock_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_address = ('localhost', 9998)
sock_tcp.bind(tcp_address)
sock_tcp.listen(1)

while True:
    print('waiting for a connection')
    connection, client_address = sock_tcp.accept()
    try:
        print('connection from', client_address)
        while True:
            data, address = sock_udp.recvfrom(4096)
            if data:
                connection.sendall(data)
            else:
                break
    finally:
        connection.close()

