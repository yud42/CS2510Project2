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
# Hearder of request file list
LIST_HEADER = '#getlist#'
# Header of add file to storage server/update information from storage or clients
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
        # (location: status) of all running storage nodes, the first one is the primary node
        # status is 1 or 0, respectively meaning ready or not.
        # location is in the format of (address, port)
        self.storage_nodes = storage_nodes
        # file list
        self.file_list = []
        # set up socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow python to use recently closed socket
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.address, self.port))
        # listen all messages
        self.s.listen(MAX_QUEUE_SIZE)
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
            self.read_File(data, connection, addr)
        elif data and data[:len(LIST_HEADER)] == LIST_HEADER:
            self.read_List(data, connection, addr)
        elif data and data[:len(UPDATE_HEADER)] == UPDATE_HEADER:
            self.add_file(data, connection, addr)
        else:
            self.disconnect(connection, addr)
            return
    
    def read_File(self, data, connection, addr):
        """
        This method handles downloads data requests from clients
        :param data: The data listened from remote clients
        :param connection: The connection server is connected to 
        :param addr: (ip address, port) of the system connected
        """
        filename = data[len(REQUEST_HEADER):]
        file_path = os.path.join(self.data_path, filename)
        
        message = DATA_HEADER.encode() + obtain(file_path) + DATA_TAIL.encode()
        connection.send(message)
        update_stats(message)

        ack = connection.recv(MAX_RECV_SIZE)
        if ack.decode() == DISCONNECT:
            self.disconnect(connection, addr)
        return
    
    def read_List(self, data, connection, addr):
        """
        This method handles show file list request from clients
        :param data: The data listened from remote clients
        :param connection: The connection server is connected to 
        :param addr: (ip address, port) of the system connected
        """
        file_list = get_list(self.data_path)
        message = encode_list_message(file_list)
        connection.send(message)
        update_stats(message)
        
        ack = connection.recv(MAX_RECV_SIZE)
        if ack.decode() == DISCONNECT:
            self.disconnect(connection, addr)
        return
    
    def add_file(self, data, connection, addr):
        """
        This method handles add file requests from clients and directory servers
        :param data: The data listened from remote connections
        :param connection: The connection server is connected to 
        :param addr: (ip address, port) of the system connected
        """
        data_body = ''
        while True:
            is_head = False
            is_tail = False
            data = connection.recv(MAX_RECV_SIZE)
            data = data.decode(COD)
            if not data:
                # means the server has failed
                print("-" * 21 + " Other side failed " + "-" * 21 + "\n")
                break
            message_contents = data
            if data[:len(UPDATE_HEADER)] == UPDATE_HEADER:
                message_contents = data[len(UPDATE_HEADER):]
                is_head = True

            if data[-len(DATA_TAIL):] == DATA_TAIL:
                message_contents = message_contents[:-len(DATA_TAIL)]
                is_tail = True

            if len(message_contents) == 0:
                print("Empty data body!")
            else:
                data_body += message_contents

            if is_tail:
                print("-"*21 + "Download Done for <" + filename + "> to " + self.data_path + "-"*21 + "\n")
                message = DISCONNECT.encode()
                connection.send(message)
                update_stats(message)
                break
        
        filename, file = decode_update_message(data_body)
        file_path = os.path.join(self.data_path, filename)
        write_data(message_contents.encode(), file_path, "wb")
        return True
    
    def disconnect(self, connection, addr):
        """
        This method is used to remove connection from current connections
        :param connection: socket we want to remove
        :param addr: (ip address, port) of the system connected
        """
        self.connections.remove(connection)
        self.peers.remove(addr)
        connection.close()
#        self.send_peers()
        print("{}, disconnected\n".format(addr))
    
    def stop(self):
        self.switch = False
        
    def close(self):
        """
        close socket
        """
        self.s.close()

class Clients:
    """
    Clients used to downlaod or upload files:
        --- connect() requests to get one storage node address from directory server
        --- Get a file list (Both 2 types of servers)
        --- Add files to storage server
    """
    pass
