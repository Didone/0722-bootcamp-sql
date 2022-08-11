import socket
import errno
import sys
import time

HEADERSIZE = 10
IP = "127.0.0.1"
PORT = 8080

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((IP, PORT))
print("Connected to {IP}:{PORT}".format(IP=IP, PORT=PORT))
client_socket.setblocking(False)

while True:
    sql = input("> ")
    if sql == 'exit':
        print("Disconnecting...")
        break
    message_header = f"{len(sql):<{HEADERSIZE}}".encode("utf-8")
    client_socket.send(message_header + sql.encode("utf-8"))
    time.sleep(1)
    try:
        while True:
            message_header = client_socket.recv(HEADERSIZE)
            if not len(message_header):
                print("Server disconnected")
                sys.exit()
            message_length = int(message_header.decode("utf-8").strip())
            message = client_socket.recv(message_length).decode("utf-8")
            print(message)
    except IOError as e:
        if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
            print("Reading error", str(e))
            sys.exit()
        continue
    except Exception as e:
        print("General error", str(e))
        sys.exit()