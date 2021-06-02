"""
Follow the instructions in each method and complete the tasks. We have given most of the house-keeping variables
that you might require, feel free to add more if needed. Hints are provided in some places about what data types
can be used, others are left to user discretion, make sure that what you are returning from one method gets correctly
interpreted on the other end.
"""
import socket
import threading

MESSAGE_SIZE = 35

# TODO: you need to order clients in the bootstrapper by increasing client_id -> piazza @536 DONE
# TODO: 1) Not delete a client upon deregistration (easiest) -> piazza @436

class p2pbootstrapper:
    def __init__(self, ip='127.0.0.1', port=8888):
        ##############################################################################
        # TODO:  Initialize the socket object and bind it to the IP and port, refer  #
        #        https://docs.python.org/3/howto/sockets.html on how to do this.     #
        ##############################################################################

        print("\nBS -> " + "init\n")

        self.boots_socket = None
        self.clients = None  # None for now, will get updates as clients register
        self.conns = []

        self.boots_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.boots_socket.bind((ip, port))


    def start_listening(self):
        ##############################################################################
        # TODO:  This function will make the BS start listening on the port 8888     #
        #        Refer to                                                            #
        #        https://docs.python.org/3/howto/sockets.html on how to do this.     #
        #        You will need to link each connecting client to a new thread (using #
        #        client_thread function below) to handle the requested action.       #
        ##############################################################################

        print("\nBS -> " + "started listening\n")

        self.boots_socket.listen(5)

        while True:
            connection, addr = self.boots_socket.accept()
            self.conns.append(connection)
            thread = threading.Thread(target=self.client_thread, args=(connection, addr))
            print("\nBS -> " + "connection accepted\n")
            thread.start()

        self.boots_socket.close()

    def client_thread(self, connection, addr):
        ##############################################################################
        # TODO:  This function should handle the incoming connection requests from   #
        #        clients. You are free to add more arguments to this function based  #
        #        on your need                                                        #
        #        HINT: After reading the input from the buffer, you can decide what  #
        #        action needs to be done. For example, if the client wants to        #
        #        deregister, call self.deregister_client                             #
        ##############################################################################

        while True:
            buf = connection.recv(MESSAGE_SIZE).decode()
            if len(buf) > 0:
                contents = buf.split('-')

                action = contents[0]
                c_id = contents[1]

                if contents[0] == 'R':
                    self.register_client(c_id, addr[0], contents[2])
                    return
                elif contents[0] == 'U':
                    self.deregister_client(c_id)
                    return
                elif contents[0] == 'L':
                    client_list = self.return_clients()
                    for client in client_list:
                        msg = "{id}-{ip}-{port}".format(id=client[0], ip=client[1], port=client[2])
                        connection.send(self.fill(msg).encode())

                    connection.send(self.fill('T').encode())
                    return

                else:
                    print("Unknown message request: {r}".format(r=contents[0]))



    def register_client(self, client_id, ip, port):
        ##############################################################################
        # TODO:  Add client to self.clients                                          #
        ##############################################################################
        if self.clients is None:
            self.clients = []

        if len(self.clients) == 0:
            self.clients.append((client_id, ip, port))
        else:
            for i in range(len(self.clients)):
                if self.clients[i][0] < client_id:
                    i += 1

            self.clients.insert(i, (client_id, ip, port))


    def deregister_client(self, client_id):
        ##############################################################################
        # TODO:  Delete client from self.clients                                     #
        ##############################################################################
        removed = False
        if self.clients is None:
            print("Error: Client list is empty.")
            return

        for i in range(0, len(client_id)):
            if self.clients[i][0] == client_id:
                #self.clients.pop(i)
                removed = True

        if not removed:
            print("Error: client not in list")




    def return_clients(self):
        ##############################################################################
        # TODO:  Return self.clients                                                 #
        ##############################################################################
        return self.clients

    def start(self):
        ##############################################################################
        # TODO:  Start timer for all clients so clients can start performing their   #
        #        actions                                                             #
        ##############################################################################
        for client in self.clients:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.connect((client[1], int(client[2])))

            temp_sock.send(self.fill('S').encode())

            temp_sock.close()



    def fill(self, message):
        return message + "-" + "!" * (MESSAGE_SIZE - len(message) - 1)
