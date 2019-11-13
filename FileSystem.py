#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This is basic package for the file system
Definition of required roles here include:
    class directory server
    class storage server
    class client
    
    required CONSTANTS
    required statistics functions
"""
'''
required CONSTANTS defined below
'''
import socket
import threading
import os
import sys
from utils import *
from collections import defaultdict


#server configs




#data transfer needs
MAX_RECV_SIZE = 1024
# encode/decode format
COD = 'utf-8'
# Header of request data to server peer
REQUEST_HEADER = '#request#'
# Header of update indexing map to indexing server
UPDATE_HEADER = '#update#'
# Header of query by the filename from the indexing server
QUERY_HEADER = '#query#'
# Header of message with location of peer based on the filename
LOCATION_HEADER = '#locat#'
# Header of data
DATA_HEADER = '#data#'
# Tail of data
DATA_TAIL = '#dataend#'
#disconnect message to end a connection
DISCONNECT = '#QUIT#'

# Maximum Queue length
MAX_QUEUE_SIZE = 5

#evaluation matrix
num_of_messages = 0
num_of_bytes = 0

def reset_stats():
    global num_of_messages
    global num_of_bytes
    num_of_messages = 0
    num_of_bytes = 0
    
def update_stats(message):
    global num_of_messages
    global num_of_bytes
    num_of_messages+=1
    num_of_bytes+=sys.getsizeof(message)
    
def get_stats():
    global num_of_messages
    global num_of_bytes
    return num_of_messages,num_of_bytes



"""
Three roles of file system
"""

class DirectoryServer:
    """
    Indexing server mainly used for:
        --- Keep directories for files
        --- Handle connect request from clients
        --- Sending propagate instructions to servers
        --- Request files to new node
        --- Backup
    """
    pass



class StorageServer:
    """
    Server contains actual files used for:
        --- Keep files
        --- Communicate with client to read/receive files
        --- newFile() notify directory server for update file list
        --- Handle propagation requests from directory servers
    """
    #ONGOING
    pass

class Clients:
    """
    Clients used to downlaod or upload files:
        --- connect() requests to get one storage node address from directory server
        --- Get a file list (Both 2 types of servers)
        --- Add files to storage server
    """
    pass
