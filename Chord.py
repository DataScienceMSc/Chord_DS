import argparse
from random import randrange
from random import choice
import hashlib
import operator
import numpy as np


#*************************************************************************************************
#**********************************GENERIC FUNCTIONS**********************************************
#*************************************************************************************************

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
        fileIdsList.append(fileId)
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

	for i in self.nodeList:
    		print "Node: " + str(i.getNodeId()) + " with predecessor: " + str(i.getPredecessor())


    def getAliveNodes(self):
        return self.aliveNodes


    def assignFilesToNodes(self,fileIdsList):
        #finds the node responsible for a fileId

	for f in fileIdsList:
            for node in self.nodeList:
                if f == node.getNodeId():
                    node.storeFileToNode(f)
                    break
                else:
                    if node.getNodeId() > f:
                        node.storeFileToNode(f)
                        break


    def findNextNode(self, f, current):
      #given the requested file (id), find the best node to pass the request
       #if file is not stored locally

        while f > (pow(2, self.m) - 1):
            f -= pow(2, self.m) - 1

        currentFingerTable=current.getFingerTable()
        print "nodeid=", current.nodeId
        print "FingerTable=", currentFingerTable
        print "m=", self.m
        for i in range(self.m-1, 0, -1):
            if currentFingerTable[i][1] > current.nodeId and currentFingerTable[i][1] <= f:
                print "next node to ask = ", currentFingerTable[i][1]
                return currentFingerTable[i][1]
        
        return currentFingerTable[self.m-1][1]


    def requestFile(self, f, node):
        #this function will be used after a node has:
        #---received a request
        #---searched it has the file requested
        #---if the file is not stored loccaly
        #    ---find the most suitable node to pass the request
        #    ---send the request to the node found above
        i=0
        currentNode=self.nodeList[i]
        while currentNode.getNodeId()!= node:
                i+=1
                currentNode=self.nodeList[i]
        if currentNode.isFileStoredLocally(f):
            return currentNode
        else:
            searchNext = self.findNextNode(f, currentNode)
            return self.requestFile(f, searchNext)
    
    def lookup(self, f, node, maxiters):
        #print "f=", f
        maxiters+=1
        
        for i in self.nodeList:
            if i.getNodeId() == node :
                currentNode = i
        print "currentNodeId", currentNode.nodeId

        currentFingerTable = currentNode.getFingerTable()

        if f > currentNode.nodeId and f <= currentFingerTable[0][1]:
            successorLast = currentFingerTable[0][1]
            print "responsibleNode=", successorLast
            return successorLast
        elif maxiters == 50:
            return -1
        else:
            if not (currentNode.isFileStoredLocally(f)):
                nextNode = self.findNextNode(f, currentNode)
                #print "nextNode looking in =", nextNode
                result = self.lookup(f, nextNode, maxiters)
                return result
            else:
                return nextNode


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

    def readFromQueue(self):
        #When it is the turn for the node to handle (send and receive) requests
        #this function should check what is present in the nodes incoming Queue( the queue containing the
        #requests that were written from others in the nodes "shared memory")
        if not self.inQueue.empty():
            fileToAsk = self.inQueue.get()
            return fileToAsk
        else:
            return -1

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

maxiters = 0
randomNodeId = choice(chord.getAliveNodes())

print "requesting from node: " + str(randomNodeId)


#valia for debug start
for i in chord.nodeList:
    if i.nodeId == randomNodeId:
        randomNode = i
firstFingerTable = randomNode.getFingerTable()
#print " FirstFingerTable = ", firstFingerTable
#valia for debug end

if randomNodeId != 10: 
    chord.lookup(10,randomNodeId, maxiters)              #valia was requestFile
else:
    print "responsibleNode = ", randomNodeId

