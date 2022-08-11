import socket
import select
from csvms.engine import Engine

eng = Engine()

HEADERSIZE = 10
IP = "127.0.0.1"
PORT = 8080

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind((IP, PORT))
print("Server started on {IP}:{PORT}".format(IP=IP, PORT=PORT))
server_socket.listen()

sockets_list = [server_socket]

clients = {}

def receive_message(client_socket):
    try:
        message_header = client_socket.recv(HEADERSIZE)
        if not len(message_header):
            return False
        message_length = int(message_header.decode("utf-8").strip())
        return {"header": message_header, "data": client_socket.recv(message_length)}
    except:
        return False

while True:
    read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)
    for notified_socket in read_sockets:
        if notified_socket == server_socket:
            client_socket, client_address = server_socket.accept()
            sockets_list.append(client_socket)
            clients[client_socket] = client_address
            print(f"Accepted new connection from {client_address[0]}:{client_address[1]}")
        else:
            message = receive_message(notified_socket)
            if message is False:
                print(f"Closed connection from {clients[notified_socket]['data'].decode('utf-8')}")
                sockets_list.remove(notified_socket)
                del clients[notified_socket]
                continue
            user = clients[notified_socket]
            print(f"Received message from {user[0]}:{user[1]}: {message['data'].decode('utf-8')}")
            try:
                response = eng.execute(message['data'].decode('utf-8'))
                if response is None:
                    response = "Syntax error!"
            except Exception as e:
                response = f"Error: {e}"
            response_header = f"{len(response.encode('utf-8')):<{HEADERSIZE}}".encode('utf-8')
            notified_socket.send(response_header + response.encode('utf-8'))

    # Exception sockets
    for notified_socket in exception_sockets:
        sockets_list.remove(notified_socket)
        del clients[notified_socket]
    
