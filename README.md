# SUFS
Seattle University File System

Ana Taylor | Puma Damdinsuren |  Mitchel Downey | Virmel Gacad 

Architecture of SUFS 

SUFS system will consist of three individual subsystems, communicating via HTTP calls with Flask servers.
The client will be how the user interacts with the system. The NameNode will maintain a list of the 
contents of each DataNode and route read requests appropriately. The DataNodes will store blocks of
files and periodically communicate to the NameNode their stored contents. 

When writing a file to the system, the client will contact the NameNode to request the space for the file.
The NameNode will reply with a list of DataNodes for the client to write to. The client will then break up
the file according to the block size and send each block to each DataNode. The DataNodes will then store 
those blocks. 

API documentation 

    NameNode.py:
        route:      '/writeFile', GET
        args:       filename - string of the name of the file to be written
        returns:    if the file already exists, returns None.
                    if the file does not exists, returns an array of size N, where N is the replication factor, containing the IP addresses of data nodes the client can write to
                    
        route:      '/readFile', GET
        args:       filename - string of the name of the file to be read
        returns:    if the file does not exist, returns None.
                    if the file does exist, returns an array of size N, where N is the replication factor, containing the IP addresses of data nodes the client can read from
                    
        route:      '/listBlocks', GET
        args:       filename - string of the name of the file to get the block locations for
        returns:    if the file does not exist, returns None.
                    if the file does exist, returns a dictionary, keyed on the block IDs, containing arrays of DataNode IDs that contain each block
                    
        route:      '/blockReport', PUT
        args:       nodeID - the ID of the DataNode sending the report
                    blockList - an array of all the block IDS the DataNode is storing
        returns:    no returns
        
    DataNode.py:
        route:      '/writeBlock', PUT
        args:       blockID - the ID of the block being written
                    block - the data block being written
        returns:    no returns
        
        route:      '/readBlock', GET
        args:       blockID - the ID of the block being requested
                    block - the data block being requested
        returns:    the data block requested
        
        route:      '/replicateBlock', PUT
        args:       blockID - the ID of the block being replicated
                    nodeIP - the address of the node the block should be written to
        returns:    no returns
	**** TEST ********** 

Technologies/Tools 

    Programming language: Python 
    Networking Framework: Flask 
    AWS SDK: AWS Boto3 
    Version Control: AWS CodeCommit 

System parameters 

    Block size = 128MB 
    Replication factor N = 3 
    Block Report/Heartbeat frequency = 30 secs 

