import socket
import errno
import sys
import time

HEADERSIZE = 10
IP = "127.0.0.1"
PORT = 8080

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((IP, PORT))
client_socket.setblocking(False)
print("Connected to {IP}:{PORT}".format(IP=IP, PORT=PORT))
print('\n-- Started SQL Server --')
print('Type "exit" to exit')
print('Type "help" to get help\n')

while True:
    sql = input("> ")
    if sql.strip() == 'help':
        print(f"\nServer Client Commands:\n\t{'help':<20} - show this help\n\t{'exit':<20} - exit the program\n\nSQL Commands:\n\t{'create table':<20} - create a table\n\t{'insert':<20} - insert into a table\n\t{'update':<20} - update a table\n\t{'select':<20} - select from a table\n\t{'select distinct':<20} - select distinct rows from a table\n\t{'delete':<20} - delete from a table\n\t{'commit':<20} - commit changes to a table\n\t{'rollback':<20} - rollback all changes\n\n\tJoins:\n\t\t{'inner join':<20} - inner join two tables\n\t\t{'left join':<20} - left join two tables\n\t\t{'right join':<20} - right join two tables\n\t\t{'full join':<20} - full join two tables\n\n REMINDER: PUT SEMICOLONS (;) BETWEEN COMMANDS\n")
        continue
    if sql.strip() == 'exit':
        print("\nDisconnecting...\n")
        break
    message_header = f"{len(sql):<{HEADERSIZE}}".encode("utf-8")
    client_socket.send(message_header + sql.encode("utf-8"))
    time.sleep(0.75)
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