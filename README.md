# SUFS
Seattle University File System

**PLEASE CHECK CHECK DESIGN DOCUMENT FOR MORE INFORMATION**

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


Technologies/Tools 

    Programming language: Python 
    Networking Framework: Flask 
    AWS SDK: AWS Boto3 
    Version Control: AWS CodeCommit 

System parameters 

    Block size = 128MB 
    Replication factor N = 3 
    Block Report/Heartbeat frequency = 30 secs 

