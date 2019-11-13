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
StorageServerPortBase = 5000
StorageServerIP = ..
DirectoryServerPortBase = 6000
DirectoryServerIP = ..


#data transfer needs
MAX_RECV_SIZE = 1024
# encode/decode format
COD = 'utf-8'
# Header of request data to server peer
REQUEST_HEADER = '#request#'
# Hearder of request file list
LIST_HEADER = '#getlist#'
# Header of update indexing map to indexing server
UPDATE_HEADER = '#update#'
# Header of query by the filename from the indexing server
QUERY_HEADER = '#query#'
# Header of message with location of peer based on the filename
LOCATION_HEADER = '#locat#'
# Header of data
DATA_HEADER = '#data#'
# Header of file list
LIST_HEADER = '#list#'
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
    Directory server mainly used for:
        --- Keep directories for files
        --- Handle connect request from clients
        --- Sending propagate instructions to servers
        --- Request files to new node
        --- Backup
    """
    def __init__(self, address, port, storage_nodes):
        self.address = address
        self.port = port
        # (location, status) of all running storage nodes, the first one is the primary node
        # status is 1 or 0, respectively meaning ready or not.
        # location is in the format of (address, port)
        self.storage_nodes = storage_nodes
        # file list
        self.file_list = set()
        # set up socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow python to use recently closed socket
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.address, self.port))
        # listen all messages
        self.s.listen(MAX_QUEUE_SIZE)
        # the number of down storage node before repairing
        self.down_num = 0
        # the number of storage nodes have already been launched, by default is 3
        self.launch_num = 3
        print("-" * 12 + "Directory Server {0:1} Running".format(address, port) + "-" * 21 + "\n")

    def connect(self):
        """

        :return: location of the primary storage node
        """
        assert self.storage_nodes[0][1] == 1, "Error: the primary node is not ready!"
        return self.storage_nodes[0][0]

    def getFileList(self):
        """

        :return: get file list
        """
        return self.file_list

    def newFile(self, file_name, file):
        self.file_list.add(file_name)
        print("Synchronizing file {0} in the storage system\n".format(file_name))
        for location, status in self.storage_nodes:
            print("Directory Server connects to storage node {0}\n".format(location))
            # set up socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # allow python to use recently closed socket
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect(location)
                s.send(encode_update_message(file_name, file))
            except socket.error:
                self.down_num += 1
                self.storage_nodes.remove((location, status))
                print("Storage node {} is down, remove it from storage list")
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            print("Directory Server disconnects from storage node {0}\n".format(location))

    def launch_new(self):
        """
        Launch a new storage node since an old one is down. Copy all the files into the new one.
        :return:
        """
        self.launch_num += 1
        port = StorageServerPortBase + self.launch_num
        #TODO: use storage node class
        # set up socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # request all files from the primary storage node
        location = self.connect()
        s.connect(location)
        s.send()



class StorageServer:
    """
    Server contains actual files used for:
        --- Keep files
        --- Communicate with client to read/receive files
        --- newFile() notify directory server for update file list
        --- Handle propagation requests from directory servers
    """
    def __init__(self, data_path, port):
        self.data_path = data_path
        
        # define a socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr = INDEX_SERVER
        self.s.bind((addr, port))
        self.s.listen(MAX_QUEUE_SIZE)
        
        self.peers = []
        self.connections = []
        
        self.switch = True
        
    def run(self):
        """
        This method is use to run the server
        This method creates a different thread for each client
        """
        self.switch = True
        while self.switch:
            connection, addr = self.s.accept()
    
            # append to the list of peers 
            self.peers.append(addr)
            #print("Peers are: {}".format(self.peers))
    #        self.send_peers()
            # create a thread for a connection
            c_thread = threading.Thread(target=self.handler, args=(connection, addr))
            c_thread.daemon = True
            c_thread.start()
            self.connections.append(connection)
            print("{}, connected\n".format(addr))
        
    def handler(self, connection, addr):
        """
        This method deals with sending info to the clients 
        This methods also closes the connection if the client has left
        :param connection: The connection server is connected to 
        :param addr: (ip address, port) of the system connected
        """
        data = connection.recv(MAX_RECV_SIZE)
        data = data.decode(COD)
        if data and data[0].lower() == DISCONNECT:
            self.disconnect(connection, addr)
            return
        elif data and data[:len(REQUEST_HEADER)] == REQUEST_HEADER:
            filename = data[len(REQUEST_HEADER):]
            file_path = os.path.join(self.data_path, filename)
            
            message = DATA_HEADER.encode() + obtain(file_path) + DATA_TAIL.encode()
            connection.send(message)
            update_stats(message)

                    #convert_to_music(self.msg)
            ack = connection.recv(MAX_RECV_SIZE)
            if ack.decode() == DISCONNECT:
                self.disconnect(connection, addr)
            return
        elif data and data[:len(LIST_HEADER)] == LIST_HEADER:
            file_list = get_list(self.data_path)
            message = encode_list_message(file_list)
            connection.send(message)
            update_stats(message)
        else:
            self.disconnect(connection, addr)
            return
            
            

class Clients:
    """
    Clients used to downlaod or upload files:
        --- connect() requests to get one storage node address from directory server
        --- Get a file list (Both 2 types of servers)
        --- Add files to storage server
    """
    pass
