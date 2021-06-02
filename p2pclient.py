"""
Follow the instructions in each method and complete the tasks. We have given most of the house-keeping variables
that you might require, feel free to add more if needed. Hints are provided in some places about what data types
can be used, others are left to students' discretion, make sure that what you are returning from one method gets correctly
interpreted on the other end. Most functions ask you to create a log, this is important
as this is what the auto-grader will be looking for.
Follow the logging instructions carefully.
"""

"""
Appending to log: every time you have to add a log entry, create a new dictionary and append it to self.log. The dictionary formats for diff. cases are given below
Registraion: (R)
{
    "time": <time>,
    "text": "Client ID <client_id> registered"
}
Unregister: (U)
{
    "time": <time>,
    "text": "Unregistered"
}
Fetch content: (Q)
{
    "time": <time>,
    "text": "Obtained <content_id> from <IP>#<Port>
}
Purge: (P)
{
    "time": <time>,
    "text": "Removed <content_id>"
}
Obtain list of clients known to a client: (O)
{
    "time": <time>,
    "text": "Client <client_id>: <<client_id>, <IP>, <Port>>, <<client_id>, <IP>, <Port>>, ..., <<client_id>, <IP>, <Port>>"
}
Obtain list of content with a client: (M)
{
    "time": <time>,
    "text": "Client <client_id>: <content_id>, <content_id>, ..., <content_id>"
}
Obtain list of clients from Bootstrapper: (L)
{
    "time": <time>,
    "text": "Bootstrapper: <<client_id>, <IP>, <Port>>, <<client_id>, <IP>, <Port>>, ..., <<client_id>, <IP>, <Port>>"
}
"""
import socket
import time
import json
import random
import threading

MESSAGE_SIZE = 35
TIME_INTERVAL = 1
SLEEP_TIME = 0.01


'''
    =======================================================================================================================

        TODO: do we need a function to make the output file do we call it in here or in the client_1.py  etc?
               we close the server socekt in there too?

        TODO: do we loop through action in here or in client_1.py?

    =======================================================================================================================
'''


class p2pclient:
    def __init__(self, client_id, content, actions):

        ##############################################################################
        # TODO: Initialize the class variables with the arguments coming             #
        #       into the constructor                                                 #
        ##############################################################################

        self.time = None

        self.client_id = client_id
        self.content = content                  # array [content_id1, content_id2...]
        self.actions = actions                  # this list of actions that the client needs to execute

        # array of touples in the ([content hash], [ID], [IP], [PORT]) format
        self.content_originator_list = []     # This needs to be kept None here, it will be built eventually

        ##############################################################################
        # TODO:  You know that in a P2P architecture, each client acts as a client   #
        #        and the server. Now we need to setup the server socket of this client#
        #        Initialize the the self.socket object on a random port, bind to the port#
        #        Refer to                                                            #
        #        https://docs.python.org/3/howto/sockets.html on how to do this.     #
        ##############################################################################

        random.seed(client_id)
        self.server_port = random.randint(9000, 9999)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("\nID-" + str(self.client_id) + " -> " + "Port: " + str(self.server_port) +  "\n")
        self.server_socket.bind(('127.0.0.1', self.server_port))                        # What IP for the client???

        ##############################################################################
        # TODO:  Register with the bootstrapper by calling the 'register' function   #
        #        Make sure you communicate the server                                #
        #        port that this client is running on to the bootstrapper.            #
        ##############################################################################

        self.status = "Initial"

        self.register()                                                        # What IP for the client???

        ##############################################################################
        # TODO:  You can set status variable based on the status of the client:      #
        #        Initial: if not yet initialized a connection to the bootstrapper    #
        #        Registered: if registered to bootstrapper                           #
        #        Unregistered: unregistred from bootstrapper, but still active       #
        #        Feel free to add more states if you need to                         #
        #        HINT: You may find enum datatype useful                             #
        ##############################################################################

        # 'log' variable is used to record the series of events that happen on the client
        # Empty list for now, update as we take actions
        # See instructions above on how to append to log
        self.log = []



    def start_listening(self):
        ##############################################################################
        # TODO:  This function will make the client start listening on the randomly  #
        #        chosen server port. Refer to                                        #
        #        https://docs.python.org/3/howto/sockets.html on how to do this.     #
        #        You will need to link each connecting client to a new thread (using #
        #        client_thread function below) to handle the requested action.       #
        ##############################################################################

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> " + "started listening\n")

        self.server_socket.listen(5)
        while True:
            socket, addr = self.server_socket.accept()
            #socket.settimeout(60)
            print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> " + "accepted a connection\n")
            threading.Thread(target=self.client_thread, args=(socket, addr)).start()

        self.server_socket.close()


    def client_thread(self, socket, addr):
        ##############################################################################
        # TODO:  This function should handle the incoming connection requests from   #
        #        other clients.You are free to add more arguments to this function   #
        #        based your need                                                     #
        #        HINT: After reading the input from the buffer, you can decide what  #
        #        action needs to be done. For example, if the client is requesting   #
        #        list of known clients, you can return the output of self.return_list_of_known_clients #
        ##############################################################################

        while True:                                              # TODO: might wanna keep this running only if the  socket is still open
            data = socket.recv(MESSAGE_SIZE)
            if len(data) > 0:
                message = data.decode()
                message = message.split("-")
                if message[0] == 'S':
                    socket.close()
                    self.start()
                    return
                elif message[0] == 'O':
                    print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> RECV: request O")
                    self.return_list_of_known_clients(socket)
                    return
                elif message[0] == 'Q':
                    print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> RECV: request Q")
                    self.return_content(socket, message[1])     # passing in only the content_ID
                    return
                elif message[0] == 'M':
                    print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> RECV: request M")
                    self.return_content_list(socket)
                    return




    def register(self, ip='127.0.0.1', port=8888):
        ##############################################################################
        # TODO:  Register with the bootstrapper. Make sure you communicate the server#
        #        port that this client is running on to the bootstrapper.            #
        #        Append an entry to self.log that registration is successful         #
        ##############################################################################

        boots_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            boots_socket.connect((ip, port))
            message = self.fill("R-" + str(self.client_id) + "-" + str(self.server_port))
            boots_socket.send(message.encode())
            boots_socket.close()
            self.status = "Registered"
            print("\nID-" + str(self.client_id) + " -> " + "registred with BS\n")

        except:
            print("\nID-" + str(self.client_id) + " -> Registration failed: connection to bootstrapper was not established\n")


        # no need to log: piazza @536
        #self.log.append({"time": self.time, "text": "Client ID " + self.client_id + " registered" })



    def deregister(self, ip='127.0.0.1', port=8888):
        ##############################################################################
        # TODO:  Deregister with the bootstrapper                                    #
        #        Append an entry to self.log that deregistration is successful       #
        ##############################################################################

        boots_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        boots_socket.connect((ip, port))
        message = self.fill("U-" + str(self.client_id))
        boots_socket.send(message.encode())
        boots_socket.close()

        print("\nID-" + str(self.client_id) + " -> " + "UNregistred with BS\n")

        self.status = "Unregistered"
        self.log.append({"time": self.time, "text": "Unregistered" })


    def start(self):
        ##############################################################################
        # TODO:  The Bootstrapper will call this method of the client to indicate    #
        #        that it needs to start its actions. Once this is called, you have to#
        #        start reading the items in self.actions and start performing them   #
        #        sequentially, at the time they have been scheduled for.             #
        #        HINT: You can use time library to schedule these.                   #
        ##############################################################################

        print("\nID-" + str(self.client_id) + " -> received start from BS\n")

        start_time = time.time()

        for current_action in self.actions:

            while time.time() - start_time < current_action["time"]:
                pass

            self.time = current_action["time"]

            if current_action["code"] == "R":
                self.register()

            elif current_action["code"] == "U":
                self.deregister()

            elif current_action["code"] == "L":
                self.query_bootstrapper_all_clients()

            elif current_action["code"] == "Q":
                self.request_content(current_action["content_id"])

            elif current_action["code"] == "P":
                self.purge_content(current_action["content_id"])

            elif current_action["code"] == "O":
                self.query_client_for_known_client(current_action["client_id"])

            elif current_action["code"] == "M":
                self.query_client_for_content_list(current_action["client_id"])



        with open("client_" + str(self.client_id)  + ".json", "w") as write_file:     # once done with actions dump logs in JSON file
            json.dump(self.log, write_file)


        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> JSON file generated")



    def query_bootstrapper_all_clients(self, ip='127.0.0.1', port=8888, disable_logs=False):
        ##############################################################################
        # TODO:  Use the connection to ask the bootstrapper for the list of clients  #
        #        registered clients.                                                 #
        #        Append an entry to self.log                                         #
        ##############################################################################
        '''
            Request type: L
            Request format: L- (!->35)
            Reply format: <clientID> - <clientIP> - <clientPort> - (!->35)
            Terminator: T- (!->35)
            Return: array of tuples [(ID, IP, PORT), ...]
        '''

        if not disable_logs:
            print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request L\n")

        boots_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        boots_socket.connect((ip, port))
        message = self.fill("L")
        boots_socket.send(message.encode())                                     # sending request type 'L' to B.S.

        request_fullfilled = False
        reply_bunlde = []

        while not request_fullfilled:                                           # receiving reply
            data = boots_socket.recv(MESSAGE_SIZE)
            if len(data) > 0:
                message = data.decode().split("-")
                if message[0] == 'T':
                    request_fullfilled = True
                else:
                    reply_bunlde.append((message[0], message[1], message[2]))

        boots_socket.close()

        if not disable_logs:
            entry = "Bootstrapper: "
            for client in reply_bunlde:
                entry = entry + "<" + client[0] + ", " + client[1] + ", " + client[2] + ">, "

            self.log.append({"time": self.time, "text":  entry[:-2]})

        if not disable_logs:
            print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request L successfull\n")

        return reply_bunlde     # format: [(clientID, clientIP, clientPort), ...]

        #TODO: what if the list received is empty???


    def query_client_for_known_client(self, client_id):
        ##############################################################################
        # TODO:  Connect to the client and get the list of clients it knows          #
        #        Append an entry to self.log                                         #
        ##############################################################################
        '''
            Request type: O
            Request format: O- (!->35)
            Reply format: <clientID> - <clientIP> - <clientPort> - (!->35)
            Terminator: T- (!->35)
            Return: array of tuples [(ID, IP, PORT), ...]
        '''

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request O\n")

        client = self.get_client_from_bs(client_id)                                  # getting client info from B.S.

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((client[1], int(client[2])))
        message = self.fill("O")
        client_socket.send(message.encode())                                    # sending request type 'O' to client with given client_id

        request_fullfilled = False
        client_list = []

        while not request_fullfilled:                                           # receiving reply
            data = client_socket.recv(MESSAGE_SIZE)
            if len(data) > 0:
                message = data.decode().split("-")
                if message[0] == 'T':
                    request_fullfilled = True
                else:
                    if (message[0], message[1], message[2]) not in client_list:
                        client_list.append((message[0], message[1], message[2]))

        client_socket.close()

        if len(client_list) == 0:
            entry = "Client " + str(client_id) + ": "
            self.log.append({"time": self.time, "text":  entry})
        else:
            entry = "Client " + str(client_id) + ": "
            for client in client_list:
                entry = entry + "<" + client[0] + ", " + client[1] + ", " + client[2] + ">, "

            self.log.append({"time": self.time, "text":  entry[:-2]})

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request O successfull\n")

        return client_list

        #TODO: what if the list received is empty???


    def return_list_of_known_clients(self, socket):
        ##############################################################################
        # TODO:  Return the list of clients known to you                             #
        #        HINT: You could make a set of <IP, Port> from self.content_originator_list #
        #        and return it.                                                      #
        ##############################################################################

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return list of clients\n")
        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> " + str(self.content_originator_list) + "\n")
        for item in self.content_originator_list:
            message = self.fill(item[1] + "-" + item[2] + "-" + item[3])     # creating a packet per known client: [ID]-[IP]-[PORT]-(!->35)
            socket.send(message.encode())                               # sending encoded message

        terminator = self.fill("T")
        socket.send(terminator.encode())                                # signaling end of trasmission

        socket.close()

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return list of clients successfull\n")



    def query_client_for_content_list(self, client_id):
        ##############################################################################
        # TODO:  Connect to the client and get the list of content it has            #
        #        Append an entry to self.log                                         #
        ##############################################################################
        '''
            Request type: M
            Request format: M- (!->35)
            Reply format: <content hash> - (!->35)
            Terminator: T- (!->35)
        '''

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request M\n")

        client = self.get_client_from_bs(client_id)                                  # getting client info from B.S.

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((client[1], int(client[2])))                           # connecting to the other client
        message = self.fill("M")
        client_socket.send(message.encode())                                    # sending request type 'M' to client with given client_id

        request_fullfilled = False
        content_list = []

        while not request_fullfilled:                                           # receiving reply
            data = client_socket.recv(MESSAGE_SIZE)
            if len(data) > 0:
                message = data.decode().split("-")
                if message[0] == 'T':
                    request_fullfilled = True
                else:
                    content_list.append(message[0])

        client_socket.close()

        entry = "Client " + str(client_id) + ": "
        for content in content_list:
            entry = entry + content + ", "

        self.log.append({"time": self.time, "text":  entry[:-2]})               # [:-2] to delete the last ", "

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request M successfull\n")

        return content_list

        #TODO: what if the list received is empty???



    def return_content_list(self, socket):
        ##############################################################################
        # TODO:  Return the content list that you have (self.content)                #
        ##############################################################################

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return list of content\n")


        for item in self.content:
            message = self.fill(item)                                             # creating a packet per content: [content_hash]-(!->35)
            socket.send(message.encode())                                       # sending encoded message

        terminator = self.fill("T")
        socket.send(terminator.encode())                                        # singnaling end of trasmission

        socket.close()

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return list of content successfull\n")




    def request_content(self, content_id):
        #####################################################################################################
        # TODO:  Your task is to obtain the content and append it to the                                    #
        #        self.content list.  To do this:                                                            #
        #        The program will first query the bootstrapper for a set                                    #
        #        of all clients. Then start sending requests for the content to each of these clients in a  #
        #        serial fashion. From any P2Pclient, it might either receive the content, or may receive    #
        #        a hint about where the content might be located. On receiving an hint, the client          #
        #        attempts that P2Pclient before progressing ahead with its current list. If content is      #
        #        not found, then it proceeds to request every P2PClient for a list of all other clients it  #
        #        knows and creates a longer list for trying various P2PClients. You can use the above query #
        #        methods to help you in fetching the content.                                               #
        #        Make sure that when you are querying different clients for the content you want, you record#
        #        their responses(hints, if present) appropriately in the self.content_originator_list       #
        #        Append an entry to self.log that content is obtained                                       #
        #####################################################################################################
        '''
            Request type: Q
            Request format: Q - <content hash> - (!->35)
            Reply format:
                    If content was found: Y - (!->35)
                    If content not found: N - (!->35)
                    If giving a hint: H - <ID> - <IP>  - <PORT> - (!->35)
        '''

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request Q\n")

        # TODO: need to order client_id inside in content_originator_list decreasing order: piazza @ 536

        clients_attempted = []  # TODO: not keeping track of this as hints don't contain client_id  but only IP and PORT?

        clients_list = self.query_bootstrapper_all_clients(disable_logs=True)   # fetching client list from B.S.

        for client in clients_list:
            if self.send_content_request(content_id, client, clients_attempted):
                self.content.append(content_id)
                break   # if the request above was successfull exit the loop

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request Q successfull\n")



    '''
        Recursive content request helper that queries any given hint before returning
        Returns:
                1 -> content was found
                0 -> content was not found
    '''
    def send_content_request(self, content_id, client, clients_attempted):
        if (client not in clients_attempted):
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> client line 521:" + str(client))

            client_socket.connect((client[1], int(client[2])))

            request = self.fill("Q-" + content_id)                                   # composing request
            client_socket.send(request.encode())                                # sending request type 'Q' to client with given client_id
            reply = client_socket.recv(MESSAGE_SIZE)                            # getting reply TODO: might be a breaking point?
            client_socket.close()

            clients_attempted.append(client)

            reply = reply.decode().split("-")

            if self.content_originator_list is None:
                self.content_originator_list = []

            print(reply[0])
            print(client)
            if reply[0] == "Y":                                                                     # the client queried has the content
                self.content_originator_list.append((content_id, client[0], client[1], client[2]))    # adding info to content_originator
                self.content_originator_list.sort(key=lambda tup: tup[1], reverse=True)             # sorting the list by descending ID

                entry = "Obtained " + content_id + " from " + client[1] + "#" + client[2]           # logging the content found and location
                self.log.append({"time": self.time, "text":  entry})
                return 1

            elif reply[0] == "N":                                                                   # content was not found and no hint given
                return 0

            elif reply[0] == "H":                                                                   # the client queried gave a hint
                hint = (reply[1], reply[2], int(reply[3]))
                print("hint")
                print(hint)
                if hint not in clients_attempted:                                                   # if hint was not already queried
                    clients_attempted.append(hint)
                    return self.send_content_request(content_id, hint, clients_attempted)                             # recursively query that hint

            else:
                return 0                                                                            # shouldn't be necessary



    def return_content(self, socket, content_id):
        '''
            Request type: Q
            Request format: Q - <content hash> - (!->35)
            Reply format:
                    If content was found: Y - (!->35)
                    If content not found: N - (!->35)
                    If giving a hint: H - <ID> - <IP> - <PORT> - (!->35)
        '''

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return content\n")

        print(str(self.client_id) + " " + str(content_id) + " " + str(self.content))

        if content_id in self.content:                                          # if self has the content send confirmation reply
            reply = self.fill("Y")

        else:                                                                   # otherwise check if there is a hint to give
            hints = [item for item in self.content_originator_list if item[0] == content_id] # filtering for content_id

            if len(hints) == 0:                                                 # if there is not matching hint
                reply = self.fill("N")                                               # send negative reply

            else:                                                               # otherwise send the first hint found
                hint = hints[0]
                reply = self.fill("H-" + hint[1] + "-" + hint[2] + "-" + hint[3])

        socket.send(reply.encode())                                             # send reply
        socket.close()

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> return content successfull\n")


    def purge_content(self, content_id):
        #####################################################################################################
        # TODO:  Delete the content from your content list                                                  #
        #        Append an entry to self.log that content is purged                                         #
        #####################################################################################################

        print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> Request P\n")

        if content_id in self.content:
            self.content.remove(content_id)

            entry = "Removed " + content_id                                     # logging the content found and location
            self.log.append({"time": self.time, "text":  entry})



    def get_client_from_bs(self, client_id):

        clients_list = self.query_bootstrapper_all_clients(disable_logs=True)   # fetching client list from B.S.
        #print("\nID-" + str(self.client_id) + "-" + str(self.time) + " -> get_client_from_bs:")
        #print(clients_list)



        client = [item for item in clients_list if item[0] == client_id]        # selecting client with correct client_id

        # client = client[0] # TODO: may be a bug: ^ might generate a list so client is list

        return client[0]



    def fill(self, message):
        return message + "-" + "!" * (MESSAGE_SIZE - len(message) - 1)


