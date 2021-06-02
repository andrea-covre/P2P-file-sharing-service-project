from p2pclient import p2pclient
import json

if __name__ == "__main__":
    content = None
    actions = None

    ##############################################################################
    # You need to perform the following tasks:                                   #
    # 1) Instantiate the client                                                  #
    # 2) Client needs to pick its serveport,bind to it                           #
    # 3) Register with bootstrapper                                              #
    # 4) STart listening on the port picked in step 2                            #
    # 5) Start executing its actions                                             #
    ##############################################################################

    ##############################################################################
    # TODO:  Read the content and actions from 2.json, and feed it into the constructor of   #
    #        the p2pclient below                                                 #
    ##############################################################################

    #############################################################################
    with open('2.json') as f:
        data = json.load(f)

    content = data['content']
    actions = data['actions']

    client = p2pclient(client_id=2, content=content, actions=actions)

    ##############################################################################
    # Now provided you have completed the steps in the p2pclient constructor     #
    # properly, steps                                                            #
    # 1, 2 and 3 are completed when you instantiate the client object            #
    # We are left with steps 4 and 5                                             #
    ##############################################################################

    ##############################################################################
    # TODO: For step 4: call clients.start_listening()                           #
        #########################
    client.start_listening()
    ##############################################################################
    # For step 5: the bootstrapper will call the start() on this client, which  #
    # will make this client start taking its actions.                            #
    ##############################################################################
