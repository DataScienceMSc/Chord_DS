import argparse
from random import randrange
from random import choice
import os.path
from random import randint
import hashlib
from operator import itemgetter
import numpy as np
from scipy.stats import powerlaw

#*************************************************************************************************
#**********************************GENERIC FUNCTIONS**********************************************
#*************************************************************************************************


# Prints to a file all the requested fileIds, along with the times
#each file id was requested.
def generatePLDistFile(N,requestList,maxNodes):
    filename="requests_"+str(N)

    if os.path.exists(filename+".txt"):
        i=1
        while True:
            temp_filename=filename+"_("+str(i)+")"
            if not os.path.exists(temp_filename+".txt"):
                filename=temp_filename
                break
            i+=1

    filename+=".txt"

    with open(filename, 'a') as the_file:
        the_file.write('Using popularity distribution from\n')
        the_file.write('scipy.stats to generate request for file Ids.\n')
        for i in range(0,maxNodes):
            the_file.write("Movie with id: "+str(i)+"appears "+str(list(requestList).count(i))+" times\n")
    the_file.close()



def randomNodeGenerator(N,maxNodes):
    randomIpsAndPorts=generateRandomIPsAndPorts(N,maxNodes)

    nodeList=[]

    #the predecessor of the first node is the last one
    previousNode=randomIpsAndPorts[-1][0]

    for i in randomIpsAndPorts:
            nodeList.append(node(i,previousNode))
            previousNode=i[0]
    return nodeList



def findMaxNodesPossible(N):
    maxNodes=1
    m=0

    while True:
        m+=1
        maxNodes= maxNodes*2
        if maxNodes >= N:
            print "Chord with m=", m ,"and maximum possible nodes: ", maxNodes
            return m,maxNodes



def hashedFilesIds(filetxt, maxNodes):
    fileIdsList = []

    for row in filetxt:
        fileId=int(int(hashlib.sha1(row).hexdigest(),16) %(maxNodes))
        if fileId not in fileIdsList and fileId !=0:
            fileIdsList.append(fileId)

    print fileIdsList
    return fileIdsList



def generateRandomIPsAndPorts(N,maxNodes):
    nodeList=[]

    for i in range(N):
        port=str(randrange(0,9))+str(randrange(0,9))+str(randrange(0,9))\
                +str(randrange(0,9))

        while True:
            exists=0

            ip=str(randrange(1,256)) + "." + str(randrange(1,256)) + "." \
                    + str(randrange(1,256)) + "." + str(randrange(1,256))
            nodeId=int(int(hashlib.sha1(ip+port).hexdigest(),16) %(maxNodes))

            for j in nodeList:
                if nodeId==j[0]:
                    exists=1
                    break
            if not exists:
                nodeList.append([nodeId,ip,port])
                break

    nodeList.sort(key=lambda x:x[0])
    return nodeList



#*************************************************************************************************
#**********************************CHORD FUNCTIONS************************************************
#*************************************************************************************************
class Chord(object):
    def __init__(self, N):

        self.m,self.maxNodes=findMaxNodesPossible(N)
	self.nodeList=randomNodeGenerator(N,self.maxNodes)
	self.aliveNodes=[i.getNodeId() for i in self.nodeList]


    def getMaxNodes(self):
        return self.maxNodes

    def getNodeList(self):
        return self.nodeList


    def getAliveNodes(self):
        return self.aliveNodes


    def assignFilesToNodes(self,fileIdsList):
        #finds the node responsible for a fileId
        #print fileIdsList
	for f in fileIdsList:
            print "file: ", f
            for counter,node in enumerate(self.nodeList):
                if f == node.getNodeId():
                    node.storeFileToNode(f)
                    print "stored in", node.getNodeId()
                    break
                else:
                    if node.getNodeId() > f:
                        node.storeFileToNode(f)
                        print "stored in", node.getNodeId()
                        break
                    elif node.getNodeId()<f and counter==len(self.nodeList):
                        self.nodeList[0].storeFileToNode(f)
                        print "stored in", node.getNodeId()
                        break


    def findNextNode(self, f, current):
      #given the requested file (id), find the best node to pass the request
       #if file is not stored locally

        while f > (pow(2, self.m) - 1):
            f -= pow(2, self.m) - 1

        currentFingerTable=current.getFingerTable()

        for i in range(self.m-1, -1, -1):
            if f <= currentFingerTable[i][1] and f > currentFingerTable[i-1][1]:
                return currentFingerTable[i-1][1]
            if currentFingerTable[i][1] < f:
                return currentFingerTable[i][1]
                print max(currentFingerTable,key=itemgetter(1))[1]
        return max(currentFingerTable,key=itemgetter(1))[1]
        #na doume ti kanei stin periptwsi pou to file einai mikrotero apo to node pou to psaxnei, kati paei strava, prepei na mpei allo ena if gia na min epistrefei ton teleutaio node giati xanei auton pou to exei
        #return currentFingerTable[self.m-1][1]


    def sendMessage(self, f, node):
        node.writeToQueue(f)


    def lookup(self, f, node):

        for i in self.nodeList:
            if i.getNodeId() == node :
                currentNode = i

        currentFingerTable = currentNode.getFingerTable()
        #print "node", node, "with ft ", currentFingerTable
        if f[0] > currentNode.getNodeId() and f[0] <= currentFingerTable[0][1]:
            successor = currentFingerTable[0][1]

            for temp in self.nodeList:
                if successor==temp.getNodeId():
                    pass
            print "File: " + str(f[0]) + " served by node: "+ str(successor)
        elif f[0] == currentNode.getNodeId() or currentNode.isFileStoredLocally(f[0]):
                print "File: " + str(f) + " served by node: "+ str(currentNode.getNodeId())
        else:
            #only applicable for the first "route" of one request
            if currentNode.getNodeId() not in f[1:]:
                f.append(currentNode.getNodeId())

            nextNode = self.findNextNode(f[0], currentNode)

            #if the file has been routed this way before,
            #do not try routing again.
            if nextNode in f[1:]:
                print "Not able to find the requested file",f[0]
                return

            currentNode.increaseMessagesRouted()
            for temp in self.nodeList:
                if nextNode==temp.getNodeId():
                    self.sendMessage(f,temp)


    def updateTables(self):
        for i in self.nodeList:
            i.updateFingerTable(self.m,self.aliveNodes)



#*************************************************************************************************
#**********************************NODE FUNCTIONS*************************************************
#*************************************************************************************************
class node(object):
    def __init__(self,lst,predecessor):
        self.nodeId=lst[0]
        self.ip=lst[1]
        self.port=lst[2]
        self.inQueue=[]
        self.fingerTable=[]
        self.fileList=[]
        self.predecessor=predecessor
        self.statDict={}
        self.messagesRouted=0
        self.messagesServed=0 #fileRequest


    def getStatDict(self):
        return self.statDict


    def getMessagesServed(self):
        return self.messagesServed


    def getMessagesRouted(self):
        return self.messagesRouted


    def increaseMessagesRouted(self):
        self.messagesRouted+=1


    def increaseMessagesServed(self):
        self.messagesServed+=1


    def getFingerTable(self):
        return self.fingerTable


    def getNodeId(self):
        return self.nodeId


    def getPredecessor(self):
        return self.predecessor


    def storeFileToNode(self,f):
        self.fileList.append(f)


    def getFileList(self):
        return self.fileList


    def isFileStoredLocally(self, requestId):
        #this method should check if the current node contains the
        #file asked
        #returns true,false
        if requestId in self.fileList:
            return True
        else:
            return False


    def writeToQueue(self,item):
        self.inQueue.append(item);


    def readFromQueue(self):
        #When it is the turn for the node to handle (send and receive) requests
        #this function should check what is present in the nodes incoming Queue( the queue containing the
        #requests that were written from others in the nodes "shared memory")
        if self.getNodeId()==5:
            print "node's",self.getNodeId()," queue content: ", self.inQueue
        if len(self.inQueue) >0:
            request = self.inQueue.pop(0)
            print "request", request
            if request[0] not in self.statDict:
                self.statDict[request[0]]=1
            else:
                self.statDict[request[0]]+=1
            return request
        else:
            return None


    def updateFingerTable(self, m, aliveNodes):
        for i in range (m):
            fingerNode = pow(2,i) + self.nodeId

            while fingerNode > (pow(2, m) - 1):
                fingerNode -= pow(2, m) - 1

            if fingerNode in aliveNodes:
                self.fingerTable.append([self.nodeId+pow(2,i),fingerNode])

            else:
                for j in aliveNodes:
                    if aliveNodes[-1] < fingerNode:
                        self.fingerTable.append([self.nodeId+pow(2,i),aliveNodes[0]])
                        break
                    if j >= fingerNode:
                        self.fingerTable.append([self.nodeId+pow(2,i),j])
                        break



#executing script using --->python Chord.py --N <number>
#if --N ... is not given, Chord DS will be initialized uning 10 nodes by default

parser=argparse.ArgumentParser("Please give number of nodes for the Chord system:")
parser.add_argument("--N","--n", type=int, help="Number of nodes present in the distributed system", default=10)
args=parser.parse_args()
print "given N: ", args.N


filetxt = open("filenamestest.txt", "r")
fileIdsList = hashedFilesIds(filetxt, args.N -1)


chord=Chord(args.N-1)
chord.assignFilesToNodes(fileIdsList)

chord.updateTables()

#randomNodeId = choice(chord.getAliveNodes())
#valia for debug start
#for i in chord.nodeList:
#    if i.nodeId == randomNodeId:
#        randomNode = i
#firstFingerTable = randomNode.getFingerTable()
#print " FirstFingerTable = ", firstFingerTable
#valia for debug end

#gia na metrisoume to chain (posa hops ekane kathe minima)
#tha stelnoume mia lista

requestList=powerlaw.rvs(1.65, size=1000, discrete=True,scale=chord.getMaxNodes())
generatePLDistFile(args.N,requestList,chord.getMaxNodes())
lst=[choice(chord.getNodeList()) for i in range(1,1000)]

for (node,request) in zip(lst,requestList):
    node.writeToQueue([request,node.getNodeId()])

#valia for debug start
#for node in chord.getNodeList():
 #   if node.getNodeId() == 9:
  #      node.writeToQueue([0,11])
#valia for debug end


counter=0
while counter <1000:
    for node in chord.getNodeList():
        request=node.readFromQueue()
        if request==None:#no pending Requests in the i-nodes Queue
            continue
        else:
            #print "start node", node.getNodeId(), "looking for file", request
            chord.lookup(request,node.getNodeId())
    counter+=1

for i in chord.getNodeList():
    print "\n\nNode: " + str(i.getNodeId())
    print "*************"
    print "Messages Routed: "+ str(i.getMessagesRouted())
    print "File Requests Served: " + str(i.getMessagesServed())
    print "Popularity dictionary: ", i.getStatDict()
    print "Node Can serve: ", i.getFileList()
