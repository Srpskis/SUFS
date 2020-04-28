"""
*   SUFS - Seattle University File System
*   Data Node
    Team 6: Ana Taylor, Puma Damdinsuren, Mitchel Downey, and Virmel Gacad
*   Winter Quarter 2020, CPSC 4910 - Cloud Computing - Dr. Dave Lillethun

"""

from flask import Flask
from flask import request
import json
import threading 
import time
import requests
import base64
import os

app = Flask(__name__)


# List of blockIDs that contains the file (or part of file if it has been divided due to its' size) 
# blockIds are represented as numbers/digits
# [0, 1, 2, ...]
heldBlocks = []

# Dictionary of block IDs and its' data size
# Key: blockId
# Value: data size (length of the string)
# {0 : 128 , 1 : 128 , ... }
blockDataLength = {}

BLOCK_SIZE = 128000000

# Helper fuction for concatenating parts of url
def url(server,port,path):
    return "http://" + str(server) + ":" + str(port) + path

# Recieves 128MB data chunck from client, decodes it and writes it to blocks.
# Keeps a list of blocks that are written  
@app.route("/data/<int:blockId>", methods=['PUT'])
def writeBlock(blockId):
    global blockDataLength
    chunkData = request.get_json().get("encodedData", "")
    decodedData = base64.b64decode(chunkData)
    blockDataLength.update({blockId : len(decodedData)})
    with open (str(blockId), 'wb') as file:
        file.write(decodedData)
        file.close()
    heldBlocks.append(blockId)
    return "Success!"


# Extracts the data stored in the blocks, encodes it, and returns it to the client
@app.route("/storedData/<int:blockId>", methods=['GET'])
def getBlock(blockId):
    blockData = None
    dlength = blockDataLength[blockId]
    with open (str(blockId), 'rb') as file: 
        blockData = file.read(dlength)
        encodedData = str(base64.b64encode(blockData, altchars=None))
    return json.dumps({"data" : encodedData})


# Return: Status code indicating if the copying of nodes was successful
@app.route("/nodeCopies/", methods=['PUT'])
def copyBlock():
    global port
    args = request.get_json()
    blockId = args.get("ID", '')
    nodeIP = args.get("IP", '')
    blockData = None
    dlength = blockDataLength[blockId]
    print(dlength)
    with open (str(blockId), 'rb') as file: 
        blockData = file.read(dlength)
        encodedData = str(base64.b64encode(blockData, altchars=None))
        print(len(blockData))   
    send_to_block_task = {"encodedData": encodedData}
    response2 = requests.put(url(nodeIP,port, "/data/" + str(blockId)),json=send_to_block_task)
    return response2.status_code

# Sends a block report containing the data node's ID and an array of its held blocks 
# every 30 seconds to the Name Node
# This method is threaded
def sendBlockReport():
    global dataNodeID
    global server
    global port
    while(True):
        if((time.time() - startTime) % 30 == 0):
            report = {"ID" : dataNodeID , "report" : heldBlocks}
            # PUT /blockReport - Name Node API     
            # Arguments: {"DataNodeID" : 0, "report" : [BlockO, Block1,...]}
            # Return: none
            response = requests.put(url(server, port, "/blockReport/"), json=report)
            if (response.status_code == 200):
                print("Success.")
            else:
                print("Error: " + response.status_code)


# Sends the data node's ID and its IP address to the name node at startup
# User provides the data nodes IP address trough the command line on startup
#  
def sendIP():
    global dataNodeID
    global server
    global port
    report = {"ID" : dataNodeID , "IP" : input("What's my IP address?: ")}
    
    # PUT /getIP - NameNode API
    #       Sends a dictionary containing data node ID and its IP address to 
    #       populate dataNodeIP dictionary in Name Node on startup.
    # Arguments: {"DataNodeID" : 1.2.3.4, ...}
    # 
    response = requests.put(url(server, port, "/IP/"), json=report)
    if response.status_code == 200:
        print("Success.")
    else:
        print("Error: " + response.status_code)


# User provides unique ID for this data node, IP of the name node, and port of the name node
dataNodeID = input("Enter unique ID for this data node: ")
server = input("NameNodeIP: ")
port = input("NameNode Port: ")
# Thread start time
startTime = time.time()
t1 = threading.Thread(target=sendBlockReport)
t1.daemon = True
t1.start()

