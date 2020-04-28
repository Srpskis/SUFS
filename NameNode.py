"""
*   SUFS - Seattle University File System
*   Name Node
    Team 6: Ana Taylor, Puma Damdinsuren, Mitchel Downey, and Virmel Gacad
*   Winter Quarter 2020, CPSC 4910 - Cloud Computing - Dr. Dave Lillethun

"""

from flask import Flask
from flask import request
from markupsafe import escape
import json
import time
import threading
import requests
import operator
from threading import Lock
import sys
app = Flask(__name__)

# Dictionary of file names and their associated blocks
# Key: name of the file
# Value: block IDs where chucks of this file is stored
# {fileName1 : [0,1,2], fileName2 : [3,4,5], ...}
fileList = {}

# Dictionary of data node IDs and their IPs
# Key = dataNode ID
# Value = dataNode IP
# { 0 : 1.0.2.5, 1 : 0.0.0.2}
dataNodeIP = {}


# Dictionary of data node IDs and the blocks they store
# Key: dataNode ID
# Value: blockIDs that are stored in this data node
# { 0 : [0, 1, 2] , 1 : [0, 1, 2] , ...}
blockStatus = {}

# Dictionary of data node IDs and the last time they have sent blockReport
# Key: dataNode ID
# Value: time (when the block report is last sent)
# {0 : 30 , 1 : 25 , ... }
timeRecord = {}

# Dictionary of block IDs and the number of replicas it has at the moment
# Key: block ID
# Value: number of replicas (from 0 to 3)
# {0 : 2 , 1 : 3 , ... }
blockCount = {}

# block replication factor
replicationFactor = 3
nextBlockIndex = 0
BLOCK_SIZE = 128000000
lock = Lock()
newBlocks = []

def url(server, port, path):
    return "http://" + str(server) + ":" + str(port) + path

# Returns:  Dictionary of BlockID's and list of DataNodes the block should store this block,  
#           where BlockID is represented as a number and DataNodeID is represented as IP.
#           {BlockID : [DataNode1IP, DataNode2IP,...],...}        
@app.route("/files/", methods=['POST'])
def blockAllocation():
    global nextBlockIndex
    global fileList
    global newBlocks
    # Contains data node IP's
    nodeList = []
    args = request.get_json()
    fileName = args.get('filename','')
    fileSize = args.get('filesize','')
    if fileName not in fileList:
        i = 0
        # Contains {dataNodeID : num blocks stored}
        numBlocksPerNode = {}
        for key,value in dataNodeIP.items():
            numBlocksPerNode.update({key : len(blockStatus[key])})
        sortedNumBlocks = sorted(numBlocksPerNode.items(), key=operator.itemgetter(1))
        for key,value in sortedNumBlocks:          
            if(i < replicationFactor):
                nodeList.append(dataNodeIP[key])
                i += 1
            else:
                break
    else:
        print("File already exists.")
        return json.dumps(None)
        sys.exit(0)

    numBlocks = fileSize / BLOCK_SIZE
    if fileSize % BLOCK_SIZE != 0:
        numBlocks += 1
    # This dictionary will store blockIds : dataNode IPs to be returned to the client
    returnList = {}
    # Stores a list of newly allocated blocks in order to update filelist
    newBlocks = []
    # generate a dictionary of blockIds : dataNode IPs to return to the client
    for block in range(0, numBlocks):
        returnList.update( {nextBlockIndex : nodeList } )
        newBlocks.append(nextBlockIndex)
        nextBlockIndex += 1
    # update the fileList dictionary with blockIDs allocated for this file
    #fileList.update({fileName : newBlocks})
    return json.dumps(returnList)


@app.route("/fileList/<fileName>", methods=['PUT'])
def updateFileList(fileName):
    global fileList
    #args = request.get_json()
    #fileName = args.get('fileName','')
    fileList.update({fileName : newBlocks})
    return "success"

# Returns: String that displays the file's blocks and the IPs of the DataNodes that hold those blocks.
#          {blockID : [dataNodeIP's]}
@app.route("/nodeIP/<fileName>", methods=['GET'])
def nodeIPs(fileName):
    returnList = {}
    if fileName in fileList:
        for block in fileList[fileName]:
            # contain dataNodeIP's
            blockList = []
            for node, blocks in blockStatus.items():
                if block in blocks:
                    blockList.append(dataNodeIP[node])
            returnList.update({block : blockList} )
    return json.dumps(returnList)
    

# Returns: dictionary of blocks and list of data nodes that store each block.
#          {blockID : [dataNodeIP1, dataNodeIP2,...],...}
@app.route("/file/<fileName>" , methods=['GET'])
def getBlockList(fileName):
    if fileName not in fileList:
        return None
    returnList = {}
    for block in fileList[fileName]:
        blockList = []
        for node, blocks in blockStatus.items():
            if block in blocks:
                blockList.append(node)
        returnList.update({block : blockList})
    return json.dumps(returnList)

# Receive a dictionary from data node containing the data node ID and list of its held blocks 
# every 30 seconds
# Updates blockStatus and timeRecord
@app.route("/blockReport/", methods=['PUT'])
def blockReport():
    global timeRecord
    global blockStatus
    args = request.get_json()
    nodeID = args.get("ID", '')
    newReport = args.get("report", '')
    lock.acquire()
    if(nodeID not in dataNodeIP):
        dataNodeIP.update({nodeID: str(request.remote_addr)})
    blockStatus[nodeID] = newReport
    timeRecord[nodeID] = time.time()
    lock.release()
    return "Success."

# Checks for data node failure. Gives a 15 second window before data node is deleted 
# from records
def heartBeat():
    global blockCount
    global blockStatus
    global timeRecord
    while(True):
        #sleep(90) gives enough time to write a file into the SUFS.
        time.sleep(90)
        for node, value in blockStatus.items():
            if (time.time() - timeRecord[node]) > 45.0:
                del blockStatus[node]
                del timeRecord[node]
                del dataNodeIP[node]
        del blockCount
        blockCount = {}
        for node, value in blockStatus.items():
            for storedBlock in value:
                if storedBlock in blockCount:
                    blockCount[int(storedBlock)] += 1
                else:
                    blockCount.update({int(storedBlock) : 1})
        for name, blockList in fileList.items():
            for blockID in blockList:
                # data node ip
                writeNode = ""
                targetNode = ""
                if blockCount[int(blockID)] != replicationFactor:
                    for node, value in blockStatus.items():
                        if blockID in value:
                            writeNode = dataNodeIP[node]
                            break
                    for node, value in blockStatus.items():
                        if blockID not in value:
                            targetNode = dataNodeIP[node]
                            blockStatus[node].append(blockID)
                            break
                    copyTask = {"ID" : blockID, "IP" : targetNode}
                    # PUT /nodeCopies/ -  DataNode API
                    #                     
                    # Arguments: 
                    # Returns: None
                    response2 = requests.put(url(writeNode,"5000", "/nodeCopies/"),json=copyTask)


# Recieve from data node dictionary containing data node ID and its IP address
# and store it to dataNodeIP dictionary 
#       dataNodeIP = {"DataNodeID" : 1.2.3.4, ...}
@app.route("/IP/", methods=["PUT"])
def getIP():
    args = request.get_json()
    dNodeID = args.get("ID", '')
    dNodeIP = args.get("IP", '')
    dataNodeIP[dNodeID] = dNodeIP
    return "Success!"


t1 = threading.Thread(target=heartBeat)
t1.daemon = True
t1.start()
