"""
*   SUFS - Seattle University File System
*   Client
    Team 6: Ana Taylor, Puma Damdinsuren, Mitchel Downey, and Virmel Gacad
*   Winter Quarter 2020, CPSC 4910 - Cloud Computing - Dr. Dave Lillethun

*   SUFS system will consist of three individual subsystems, communicating via HTTP calls with Flask servers. 
    The client will be how the user interacts with the system. The NameNode will maintain a list of the contents 
    of each DataNode and route read requests appropriately. The DataNodes will store blocks of files and periodically 
    communicate to the NameNode their stored contents. 

    When writing a file to the system, the client will contact the NameNode to request the space for the file. 
    The NameNode will reply with a list of DataNodes for the client to write to. The client will then break up the 
    file according to the block size and send each block to each DataNode. The DataNodes will then store those blocks. 

    Similarly, when reading a file, the client will contact the NameNode to request a list of DataNodes that hold the 
    blocks for the requested file. The NameNode will reply with a list of the block IDs that the file was broken into 
    and the IP addresses of those DataNodes that store each block. The client will then request each block from one 
    DataNode and combine those blocks back into the complete file. 

"""

import sys
import requests
import boto3
import botocore
import json
import os
import base64
from os import environ

# Size of each block in bytes (128MB)
BLOCK_SIZE = 128000000 

# Helper fuction for concatenating parts of url
def url(server, port, path):
    return "http://" + str(server) + ":" + str(port) + path

# Function for option 1: File writing
# Arguments:    - fileName: Name of file to be written
#               - fileSize: Size of file to be written
#               - server:   Data Node server IP
#               - port:     Data Node port (5000)
def write(fileName, fileSize, server, port):

    print("You chose to write a file called " + fileName)
    # POST /blockAllocation - Name Node API
    #         Sends name and size of file in order for Name Node
    #         to generate blockInfo report containing the list of Data Nodes that should store
    #         this block. 
    # Arguments: {"filename" : fileName, "fileSize" : fileSize}
    # Returns:  Dictionary of BlockID's and list of DataNodes the block should store this block,  
    #           where BlockID is represented as a number and DataNodeID is represented as IP.
    #           allocatedBlocks = {BlockID : [DataNode1IP, DataNode2IP,...],...}
    #   
    write_task = {"filename":fileName, "filesize":fileSize}
    allocatedBlocks = requests.post(url(server, port,"/files/"), json=write_task)
    if allocatedBlocks.json():
        if allocatedBlocks.status_code == 200:
            print("Success!")
        else:
            print("Error: " + str(allocatedBlocks.status_code))
            sys.exit(0)
        returnedBlocks = allocatedBlocks.json()
        blockList = list()
        for eachBlock, dnIP in returnedBlocks.items():
            blockList.append(int(eachBlock)) 
        blockList.sort()
        with open(fileName, 'rb') as file:
            for eachBlock in blockList:
                IPList = returnedBlocks[str(eachBlock)]
                if IPList:
                    data = file.read(BLOCK_SIZE)
                    for IP in IPList:
                        base64EncodedData = str(base64.b64encode(data, altchars=None))
                        send_to_block_task = {"encodedData": base64EncodedData}
                        # PUT /data/blockID - DataNode API
                        #                     parses block report that Name Node has returned 
                        #                     and distribute blocks according to the report
                        # Arguments: base64 encoded data chucks each of block size (128MB) {"encodedData": base64EncodedData}
                        # Returns: None
                        response2 = requests.put(url(IP,port, "/data/" + str(eachBlock)),json=send_to_block_task)
                        if (response2.status_code == 200):
                            print("Success. " + str(eachBlock) + " now in data node " + IP)
                        else:
                            print("Error: " + response2.status_code)
                else:
                    file.seek(BLOCK_SIZE, os.SEEK_CUR)
        updateFileList = {"fileName" : fileName}
        response = requests.put(url(server,port,"/fileList/" + fileName))
    else:
        print("File already exists.")


# Function for option 2: File reading
# Arguments:    - fileName: Name of file to be read
#               - server:   Data Node server IP
#               - port:     Data Node port (5000)
def read(fileName, server, port):
    print("You chose to read a file called " + fileName)
    # stores blocks that contains the file
    blockList = list()
    # GET /nodeIP/fileName - NameNode API
    # Arguments: none
    # Returns: dictionary that displays the file's blocks and the IPs of the DataNodes that hold those blocks.
    #          {blockID : [dataNodeIP's]}
    nodeList = requests.get(url(server,port,"/nodeIP/" + fileName))
    if nodeList.status_code == 200:
        nodes = nodeList.json()
        if nodeList is not None:
            for eachBlock, dnIP in nodes.items():
                blockList.append(int(eachBlock)) 
            blockList.sort()
            with open("NEW_" + fileName, "ab+") as file:
                for eachBlockId in blockList:
                    dataNodeIP = nodes[str(eachBlockId)][0]
                    # GET /storedData/blockID - Data Node API
                    #                           reads the file blockID and returns it to the client
                    # Arguments: none
                    # Returns: base64 encoded data chucks each of block size or smaller (128MB)
                    #          {"encodedData" : base64EncodedData} 
                    response = requests.get(url(dataNodeIP, port, "/storedData/"+ str(eachBlockId)))
                    jsonResponse = response.json()
                    decodedData = base64.b64decode(jsonResponse['data'])
                    file.write(decodedData)
    elif nodeList.status_code == 404:
        print("That file does not exist.")
    else:
        print(str(nodeList.status_code))
    
# Function for option 3: Displays the list of blocks and data nodes that store specific file
# Arguments:    - fileName: Name of file to be read
#               - server:   Data Node server IP
#               - port:     Data Node port (5000)
def listDataNode(fileName, server, port):
    print("You chose to list data nodes storing file " + fileName + " in blocks.")
    # GET /getBlocks/fileName - NameNode API
    # Arguments: fileName
    # Returns: dictionary of blocks and list of data nodes that store each block.
    #          {blockID : [dataNodeIP1, dataNodeIP2,...],...}
    nodeList = requests.get(url(server,port,"/file/" + fileName))
    for block, nodes in nodeList.json().items():
        print("Block" + block + ": " + str(nodes).strip("[]"))

def main(args):
    while True: 
        if len(args) != 3:
            print("\n\n\n\nInvalid arguments!")
            print("Command line instruction to run client: python3 client.py <name node server ip> <name node port>")
            break
        else:
            server = str(args[1])
            port = str(args[2])
            print("\n\n\n\nChoose: \n 1.Write File to SUFS\n 2.Read File from SUFS \n 3.List Data Nodes that store file blocks \n 4.Exit")
            choice = input("Choice: ")
            if choice == 4:
                print("Exiting...")
                sys.exit(0)
            if (choice is not 1 and choice is not 2 and choice is not 3):
                print("Invalid Command. Please try again.") 
                continue
            file_name_input = input("Enter file name: ")
            fileName = file_name_input
            if choice == 1:
                bucket_name = input("Enter S3 bucket name: ")
                resource = boto3.resource('s3')
                try:
                    resource.Bucket(bucket_name).download_file(fileName, fileName)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
                s3 = boto3.client('s3')
                response = s3.head_object(Bucket = bucket_name, Key = fileName)
                fileSize = response['ContentLength']
                write(fileName, fileSize, server, port)
            elif choice == 2:
                read(fileName, server, port)
            elif choice == 3:
                listDataNode(fileName, server, port)
            else:
                print("Invalid input.")
    
if __name__ == '__main__':
    main(sys.argv)

