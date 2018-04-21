import argparse
from random import randrange
import hashlib



def findMaxNodesPossible(N):
    maxNodes=1
    while True:
        maxNodes= maxNodes*2
        if maxNodes >= N:
            return maxNodes



def generateRandomIPsAndPorts(N):
    #given the number of nodes needed for the DS
    #return a list of [nodeId, ip, port]
    #orderd based on nodeId, where:
    #ip and port are randomly generated strings
    #nodeId is the result when hashing id+port (concatenation)
    #Should also check that the random ips are unique

    nodeList=[]
    maxNodes=findMaxNodesPossible(N)

    print "Maximum possible nodes: ", maxNodes

    for i in range(N):
        port=str(randrange(0,9))+str(randrange(0,9))+str(randrange(0,9))+str(randrange(0,9))

        while True:
            exists=0

            ip=str(randrange(1,256)) + "." + str(randrange(1,256)) + "." + str(randrange(1,256)) + "." + str(randrange(1,256))

            for i in nodeList:
                if ip == i[0]:
                    exist=1
                    break
            if not exists:
                break
        nodeId=int(hashlib.sha1(ip+port).hexdigest(),16) %maxNodes
        nodeList.append([nodeId,ip,port])
    return nodeList


class node(object):
    def __init__(self,lst):
        self.nodeId=lst[0]
        self.ip=lst[1]
        self.port=lst[2]
        self.inQueue=[]
        self.fingerTable=[]


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
print "given N: ", args.N
randomIpsAndPorts=generateRandomIPsAndPorts(args.N)

nodeList=[]
for i in randomIpsAndPorts:
    nodeList.append(node(i))

for i in nodeList:
    print "Generated Node: " + str(i.nodeId) + "---->" + str(i.ip) +":" +str(i.port)

