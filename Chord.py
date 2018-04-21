import argparse

def findMaxNodesPossible():
    pass



def generateRandomIpsAndPorts(N):
    #given the number of nodes needed for the DS
    #return a list of [nodeId, ip, port]
    #orderd based on nodeId, where:
    #ip and port are randomly generated strings
    #nodeId is the result when hashing id+port (concatenation)
    #Should also check that the random ips are unique

    NodeList=[]
    findMaxNodesPossible(N)



class node(object):
    def init(self,id):
        nodeId=id
        inQueue=[]
        fingerTable=[]


    def isFileStoredLocally(requestId):
        #this method should check if the current node contains the
        #file asked
        #returns true,false
        pass

    def readFromQueue():
        #When it is the turn for the node to handle (send and receive) requests
        #this function should check what is present in the nodes incoming Queue( the queue containing the
        #requests that were written from others in the nodes "shared memory"
        pass

    def requestFile():
        #this function will be used after a node has:
        #---received a request
        #---searched it has the file requested
        #---if the file is not stored loccaly
        #    ---find the most suitable node to pass the request
        #    ---send the request to the node found above
        pass

    def findNextNode(f):
        #given the requested file (id), find the best node to pass the request
        #if file is not stored locally
        pass

    def updateFingerTable():
        pass


#executing script using --->python Chord.py --N <number>
#if --N ... is not given, Chord DS will be initialized uning 10 nodes by default



parser=argparse.ArgumentParser("Please give number of nodes for the Chord system:")
parser.add_argument("--N","--n", type=int, help="Number of nodes present in the distributed system", default=10)
args=parser.parse_args()

createRandomIPs(args.N)
