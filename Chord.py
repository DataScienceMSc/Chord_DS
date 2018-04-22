import argparse
from random import randrange
import hashlib
import operator



def findMaxNodesPossible(N):
    maxNodes=1
    while True:
        maxNodes= maxNodes*2
        if maxNodes >= N:
            return maxNodes

#valia opoia sunartisi exei mesa klaseis den exei testaristei opws h parakatw :p
def assignFileToNode(fileIdsList, aliveNodes):
    #finds the node responsible for a fileId
    for f in fileIdsList:
        #print "fileid", f
        for node in aliveNodes:
            if str(f) == str(node.nodeId):
                node.fileList.append(str(f))
                #print f, node.nodeId
                break
            else:
                
                #for node in aliveNodes:
                    #if f > aliveNodes[-1].nodeId:
                     #   aliveNodes[0].nodeId.fileList.append[f]
                      #  break
                if node.nodeId > f:
                    node.fileList.append(str(f))
                    #print f, node.nodeId, "else"
                    break

def hashedFilesIds(filetxt, maxNodes):
    fileIdsList = []
    for row in filetxt:
        fileId=int(hashlib.sha1(row).hexdigest(),16) %(maxNodes-1)
        fileIdsList.append(fileId)
    return fileIdsList

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
        nodeId=int(hashlib.sha1(ip+port).hexdigest(),16) %(maxNodes-1)
        nodeList.append([nodeId,ip,port])
        
    return nodeList


class node(object):
    def __init__(self,lst):
        self.nodeId=lst[0]
        self.ip=lst[1]
        self.port=lst[2]
        self.inQueue=[]
        self.fingerTable=[]
        self.fileList=[]


    def isFileStoredLocally(requestId, fileList):
        #this method should check if the current node contains the
        #file asked
        #returns true,false
        if requestId in fileList:
            return True
        else:
            return False

    def readFromQueue(inQueue):
        #When it is the turn for the node to handle (send and receive) requests
        #this function should check what is present in the nodes incoming Queue( the queue containing the
        #requests that were written from others in the nodes "shared memory")
        if not inQueue.empty():
            fileToAsk = inQueue.get()
            return fileToAsk
        else:
            return -1

    def requestFile(f, node):
        #this function will be used after a node has:
        #---received a request
        #---searched it has the file requested
        #---if the file is not stored loccaly
        #    ---find the most suitable node to pass the request
        #    ---send the request to the node found above
        if isFileStoredLocally(f, node.fileList):
            return node.nodeId
        else:
            nextRequestedNode = findNextNode(f, node.fingertable)
            requestFile(f, nextRequestedNode)

#valia den eimai sigouri oti prepei na mpei mesa stin klasi
    def findNextNode(f, fingerTable, m):
    #given the requested file (id), find the best node to pass the request
    #if file is not stored locally
        while f > (pow(2, m) - 1):
            f -= pow(2, m) - 1
        
        if f in fingerTable:
            nextNode = f
        else:        
            for node in fingerTable:
                if f > aliveNodes[-1]:
                    nextNode = aliveNodes[0]
                    break
                if node > f:
                    nextNode = node
                    break
        return nextNode

    def updateFingerTable(fingerTable, m, aliveNodes, nid):
        for i in range (m):
            fingerNode = pow(2,i) + nid
        
            while fingerNode > (pow(2, m) - 1):
                fingerNode -= pow(2, m) - 1
            
            if fingerNode in aliveNodes:
                fingerTable.append(fingerNode)
           
            else:
                for j in aliveNodes:
                    print 'j=', j
                    if aliveNodes[-1] < fingerNode:
                        fingerTable.append(aliveNodes[0])
                        break
                    if j > fingerNode:
                        fingerTable.append(j)
                        break
                    
        return fingerTable
      


#executing script using --->python Chord.py --N <number>
#if --N ... is not given, Chord DS will be initialized uning 10 nodes by default

parser=argparse.ArgumentParser("Please give number of nodes for the Chord system:")
parser.add_argument("--N","--n", type=int, help="Number of nodes present in the distributed system", default=10)
args=parser.parse_args()
print "given N: ", args.N

randomIpsAndPorts=generateRandomIPsAndPorts(args.N)
print randomIpsAndPorts
nodeList=[]
for i in randomIpsAndPorts:
    nodeList.append(node(i))

nodeList = sorted(nodeList, key=operator.attrgetter('nodeId'))

for i in nodeList:
    print "Generated Node: " + str(i.nodeId) + "---->" + str(i.ip) +":" +str(i.port)

filetxt = open("/home/mscuser/Downloads/filenamestest.txt", "r")
fileIdsList = hashedFilesIds(filetxt, args.N -1)

assignFileToNode(fileIdsList, nodeList)
for node in nodeList:
    print node.fileList

