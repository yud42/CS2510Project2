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
StorageServerIP = '136.142.227.11'  #hydrogen.cs.pitt.edu
#StorageServerIP = '127.0.0.1'
DirectoryServerPortBase = 6000
DirectoryServerIP = '136.142.227.10'  #oxygen.cs.pitt.edu
#DirectoryServerIP = '127.0.0.1'



#data transfer needs
MAX_RECV_SIZE = 1024
# encode/decode format
COD = 'utf-8'
# Header of request data to server peer
REQUEST_HEADER = '#request#'
# Hearder of request file list
GETLIST_HEADER = '#getlist#'
# Header of query by the filename from the indexing server
QUERY_HEADER = '#query#'
# Header of message with location of peer based on the filename
LOCATION_HEADER = '#locat#'
# Header of data, Header of add file to storage server/update information from storage or clients
DATA_HEADER = '#data#'
# Header of file list
LIST_HEADER = '#list#'
# Tail of data
DATA_TAIL = '#dataend#'
#disconnect message to end a connection
DISCONNECT = '#QUIT#'
#ERROR from client indicate primary directory server down
DIR_ERROR = '#DIRFAIL#'
#ERROR from client indicate primary storage server down
STORAGE_ERROR = '#STORAGEFAIL#'

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
        self.start = True
        print("-" * 12 + "Directory Server {0} : {1} Running".format(address, port) + "-" * 21 + "\n")

    def connect(self):
        """

        :return: location of the primary storage node
        """
        assert self.storage_nodes[0][1] == 1, "Error: the primary node is not ready!\n"
        return self.storage_nodes[0][0]

    def getLocation(self, connection, addr):
        location = self.connect()
        msg = encode_location_message(location).encode(COD)
        connection.send(msg)
        update_stats(msg)

        ack = connection.recv(MAX_RECV_SIZE)
        if ack.decode(COD) == DISCONNECT:
            self.disconnect(connection, addr)
        return

    def getFileList(self, connection, addr):
        """
        Send complete file list and send it to "connection".
        :return:
        """
        file_list = self.file_list
        message = encode_list_message(file_list).encode(COD)
        connection.send(message)
        update_stats(message)
        ack = connection.recv(MAX_RECV_SIZE)
        if ack.decode(COD) == DISCONNECT:
            self.disconnect(connection, addr)
        return

    def detect_storage_node_down(self, location, status):
        """

        :param location: the location of the failed storage node
         :param status: the status of the failed storage node
        :return:
        """
        self.down_num += 1
        self.storage_nodes.remove((location, status))
        print("Storage node {} is down, remove it from storage list\n")
        print("Starting a new storage node ...\n")
        self.launch_new_sn()

    def newFile(self, data, connection, addr):
        data_body = ''
        data = data[len(DATA_HEADER):]
        is_tail = False
        while True:
            if not data:
                # means the server has failed
                print("-" * 21 + " Other side failed " + "-" * 21 + "\n")
                break
            message_contents = data

            if data[-len(DATA_TAIL):] == DATA_TAIL:
                message_contents = message_contents[:-len(DATA_TAIL)]
                is_tail = True

            if len(message_contents) == 0:
                print("Empty data body!")
            else:
                data_body += message_contents

            if is_tail:
                message = DISCONNECT.encode()
                connection.send(message)
                update_stats(message)
                break

            # listen to following message
            data = connection.recv(MAX_RECV_SIZE)
            data = data.decode(COD)

        file_name, file, location_receive = decode_update_message(data_body)

        self.file_list.add(file_name)
        print("Synchronizing file {0} in the storage system\n".format(file_name))
        for location, status in self.storage_nodes:
            if location == location_receive:
                continue
            print("Directory Server is connecting to storage node {0}\n".format(location))
            # set up socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # allow python to use recently closed socket
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect(location)
                print("Directory Server is connected to storage node {0}\n".format(location))
                msg = encode_update_message(file_name, file, (self.address, self.port)).encode(COD)
                s.send(msg)
                update_stats(msg)
            except socket.error:
                self.detect_storage_node_down(location, status)
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            print("Directory Server disconnects from storage node {0}\n".format(location))
        print("Synchronized file {0} in the storage system\n".format(file_name))

    def launch_new_sn(self):
        """
        Launch a new storage node since an old one is down. Copy all the files into the new one.
        :return:
        """
        self.launch_num += 1
        new_port = StorageServerPortBase + self.launch_num
        # launch new storage server
        new_data_path = "data_" + str(self.launch_num)
        new_storage_server = StorageServer(data_path=new_data_path, port=new_port)
        new_storage_server.run()
        self.storage_nodes.append(((StorageServerIP, new_port), 0))
        print("New storage node ({0}:{1}) is started. Copying files to the new storage node ...\n".format(new_data_path, new_port))
        for file_name in self.file_list:
            # set up socket for requesting files
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            while True:
                # request files from the primary storage node
                location = self.connect()
                try:
                    s1.connect(location)
                    break
                except socket.error:
                    # the primary storage node is down
                    self.detect_storage_node_down(location, 1)

            message = encode_request_message(file_name).encode(COD)
            s1.send(message)
            update_stats(message)

            # send files to the new storage node
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s2.connect((StorageServerIP, new_port))
            except socket.error:
                # the new storage node is down
                self.detect_storage_node_down((StorageServerIP, new_port), 0)
                return

            while True:
                data = s1.recv(MAX_RECV_SIZE)
                s2.send(data)
                update_stats(data)
                if data.decode(COD)[-len(DATA_TAIL):] == DATA_TAIL:
                    # is tail
                    break
            ack = DISCONNECT.encode(COD)
            s1.send(ack)
            update_stats(ack)
            msg = s2.recv(MAX_RECV_SIZE)
            if msg.decode(COD) == DISCONNECT:
                pass
            else:
                print("Unrecognized message received by directory server: {}".format(msg))
            s1.shutdown(socket.SHUT_RDWR)
            s1.close()
            s2.shutdown(socket.SHUT_RDWR)
            s2.close()
        # change status to 1
        index = self.storage_nodes.index(((StorageServerIP, new_port), 0))
        self.storage_nodes[index] = ((StorageServerIP, new_port), 1)

    def handler(self, connection, addr):
        """
        This method deals with listening requests and replying from the clients or storage nodes
        This methods also closes the connection if the client has left
        :param connection: The connection server is connected to
        :param addr: (ip address, port) of the system connected
        """
        data = connection.recv(MAX_RECV_SIZE)
        data = data.decode(COD)
        if data and data == DISCONNECT:
            self.disconnect(connection, addr)
            return
        elif data and data[:len(GETLIST_HEADER)] == GETLIST_HEADER:
            self.getFileList(connection, addr)
        elif data and data[:len(QUERY_HEADER)] == QUERY_HEADER:
            self.getLocation(connection, addr)
        elif data and data == STORAGE_ERROR:
            self.detect_storage_node_down(self.connect(), 1)
        elif data and data[:len(DATA_HEADER)] == DATA_HEADER:
            self.newFile(data, connection, addr)

        else:
            print("Unrecognized message received by directory server: {}".format(data))
            self.disconnect(connection, addr)
            return

    def run(self):
        """
        This method is use to run the server
        This method creates a different thread for each connection
        """
        while self.start:
            connection, addr = self.s.accept()
            # create a thread for a connection
            c_thread = threading.Thread(target=self.handler, args=(connection, addr))
            c_thread.daemon = True
            c_thread.start()
            print("{0}, connected to directory server {1}\n".format(addr, self.port))

    def stop(self):
        self.start = False
        print("Stop the directory server {0}".format(self.port))

    def disconnect(self, connection, addr):
        """
        This method is used to disconnect the current connection
        :param connection: socket we want to disconnect
        :param addr: (ip address, port) of the system connected
        """
        connection.close()
        print("{0}, disconnected from the directory server {1}\n".format(addr, self.port))


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
        self.addr = StorageServerIP

        self.port = port
        self.s.bind((self.addr, port))
        self.s.listen(MAX_QUEUE_SIZE)
        
        self.dir_ip = DirectoryServerIP
        self.dir_port = DirectoryServerPortBase + 1
        
        self.peers = []
        self.connections = []
                
        self.switch = True
        print("-" * 12 + "Storage Server {0} : {1} Running".format(addr, port) + "-" * 21 + "\n")
        
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
        if data and data == DISCONNECT:
            self.disconnect(connection, addr)
            return
        elif data and data[:len(REQUEST_HEADER)] == REQUEST_HEADER:
            self.read_File(data, connection, addr)
        elif data and data[:len(LIST_HEADER)] == LIST_HEADER:
            self.read_List(data, connection, addr)
        elif data and data[:len(DATA_HEADER)] == DATA_HEADER:
            self.get_file(data, connection, addr)
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
        print(filename)
        file_path = os.path.join(self.data_path, filename)
        print(file_path)
        
        message = DATA_HEADER.encode(COD) + obtain(file_path) + DATA_TAIL.encode(COD)
        print(message)
        connection.send(message)
        update_stats(message)

        ack = connection.recv(MAX_RECV_SIZE)
        if ack.decode(COD) == DISCONNECT:
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
        if ack.decode(COD) == DISCONNECT:
            self.disconnect(connection, addr)
        return
    
    def get_file(self, data, connection, addr):
        """
        This method handles add file requests from clients and directory servers
        :param data: The data listened from remote connections
        :param connection: The connection server is connected to 
        :param addr: (ip address, port) of the system connected
        """
        data_body = ''
        data = data[len(DATA_HEADER):]        
        is_tail = False   
        while True:
            if not data:
                # means the server has failed
                print("-" * 21 + " Other side failed " + "-" * 21 + "\n")
                break
            message_contents = data

            if data[-len(DATA_TAIL):] == DATA_TAIL:
                message_contents = message_contents[:-len(DATA_TAIL)]
                is_tail = True

            if len(message_contents) == 0:
                print("Empty data body!")
            else:
                data_body += message_contents

            if is_tail:
                message = DISCONNECT.encode()
                connection.send(message)
                update_stats(message)
                break
            
            #listen to following message
            data = connection.recv(MAX_RECV_SIZE)
            data = data.decode(COD)
            
        
        filename, file, address = decode_update_message(data_body)
        if address[0] != self.dir_ip or address[1] != self.dir_port:
            self.addFile(filename, file)
        file_path = os.path.join(self.data_path, filename)
        write_data(file.encode(COD), file_path, "wb")
        print("-"*21 + "Download Done for <" + filename + "> to " + self.data_path + "-"*21 + "\n")
        return True
    
    def addFile(self, filename, file):
        """
        This method is used to notify directory server to add a file to system
        :param filename: The filename 
        :param file: The file should be add
        """
        try:
            so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            so.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            so.connect((self.dir_ip, self.dir_port))  
        except socket.error:
            self.dir_port += 1
            so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            so.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            so.connect((self.dir_ip, self.dir_port))
            print('Storage node: Error seen when connecting to directory server!')
            message = DIR_ERROR.encode()
            so.send(message)
            update_stats(message)
        
        message = encode_update_message(filename, file, (self.addr, self.port)).encode(COD)
        so.send(message)
        update_stats(message)        
        so.shutdown(socket.SHUT_RDWR)
        so.close()
    
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
        print("Stop the storage node {0}".format(self.port))
        
    def close(self):
        """
        close socket
        """
        self.s.close()


class Clients:
    """
    Clients used to download or upload files:
        --- connect() requests to get one storage node address from directory server
        --- Get a file list (Both 2 types of servers)
        --- Add files to storage server
    """
    
    def __init__(self, data_path, port):
        """
        Initialize client with remote server address and port number
        
        """
        # set up socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow python to use recently closed socket
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.locations = None
        self.data_path = data_path
        self.port = port  # server port
        
        self.dir_ip = DirectoryServerIP
        self.dir_port = DirectoryServerPortBase + 1
        
        self.file_list = None
        
    def send_error(self, dirError):
        """
        This method is used to notify directory servers errors
        :param dirError: True/False indicates wheather client get problem on connect
                         to Directory server/Storage server. 
                         True---Errors happened during connecting to directory server
                         False---Errors happened during connecting to storage server
        """
        if dirError:
            print('Clients: Error seen when connecting to directory server!')
            self.dir_port += 1
            self.s.connect((self.dir_ip, self.dir_port))
            message = DIR_ERROR.encode()
            self.s.send(message)
            update_stats(message)
        else:
            print('Clients: Error seen when connecting to storage server!')
            self.build_connection(isDir=True)
            message = STORAGE_ERROR.encode()
            self.s.send(message)
            update_stats(message)
        self.close()
        self.open_socket()
        
    def build_connection(self, isDir):
        """
        This method is used to build actual connection with directory/storage server
        :param isDir: True/False indicates wheather client connects to Directory server/Storage server. 
                         True---connecting to directory server
                         False---connecting to storage server
        """
        if isDir:
            try:
                self.s.connect((self.dir_ip, self.dir_port))
            except socket.error as e:
                self.send_error(dirError=True)
        else:
            if self.locations == None:
                print("No storage node known!")
            else:
                try:
                    addr, port = self.locations
                    self.s.connect((addr, port))
                except socket.error:
                    self.send_error(dirError=False)
        

    def connect(self):
        """
        This method is used to get the location of primary storage node from directory server
        """
        self.build_connection(isDir=True)
        
        query_message = QUERY_HEADER.encode(COD)
        self.s.send(query_message)  # send query message
        update_stats(query_message)
        try:
            data = self.s.recv(MAX_RECV_SIZE)  # read message
            data = data.decode(COD)
            if data[:len(LOCATION_HEADER)] == LOCATION_HEADER:
                self.locations = decode_location_message(data[len(LOCATION_HEADER):])
                print("Got location of primary storage node: {0}\n".format(self.locations))
        except Exception:
            print("Failed to query file location: disconnected to the indexing server.")
        
        self.close()
        self.open_socket()
        return self.locations
      
    def get_FileList(self, isDir):
        """
        This method is used to ask file list from directory/storage server
        :param isDir: True/False indicates wheather client ask from Directory server/Storage server. 
                         True---ask from directory server
                         False---ask from storage server
        """
        self.build_connection(isDir)
        message = GETLIST_HEADER.encode(COD)
        self.s.send(message)
        update_stats(message)
        try:
            data = self.s.recv(MAX_RECV_SIZE)  # read message
            data = data.decode(COD)
            if data[:len(LIST_HEADER)] == LIST_HEADER:
                self.file_list = decode_list_message(data[len(LIST_HEADER):])
                print("Got file list: {0}\n".format(self.file_list))
        except Exception:
            print("Failed to query file list: disconnected to the remote server.")
            
        
        self.close()
        self.open_socket()
        return self.file_list
                
    def readFile(self, filename):
        """
        This method is used to get file from storage node invoked by client
        :param filename: The file name requested.
        """
        self.build_connection(isDir=False)
        
        message = encode_request_message(filename).encode(COD)
        self.s.send(message)
        update_stats(message)
        print(message)
        data_body = ''
        while True:
            is_tail = False
            data = self.s.recv(MAX_RECV_SIZE)
            data = data.decode(COD)
            if not data:
                # means the server has failed
                print("-" * 21 + " Other side failed " + "-" * 21 + "\n")
                break
            message_contents = data
            if data[:len(DATA_HEADER)] == DATA_HEADER:
                message_contents = data[len(DATA_HEADER):]

            if data[-len(DATA_TAIL):] == DATA_TAIL:
                message_contents = message_contents[:-len(DATA_TAIL)]
                is_tail = True

            if len(message_contents) == 0:
                print("Empty data body!")
            else:
                data_body += message_contents

            if is_tail:
                message = DISCONNECT.encode()
                self.s.send(message)
                update_stats(message)
                break
        
        file = data_body.decode(COD)
        file_path = os.path.join(self.data_path, filename)
        write_data(file.encode(COD), file_path, "wb")
        print("-"*11 + "Download Done for <" + filename + "> to " + self.data_path + "-"*11 + "\n")
        self.close()
        self.open_socket()
        return True
        
    def addFile(self, filename, file):
        """
        This method is used to add file from storage node invoked by client
        :param filename: The file name added
        :param file: The file added
        """
        self.build_connection(isDir=False)
        message = encode_update_message(filename, file, ('1','2')).encode(COD)

        self.s.send(message)
        update_stats(message)
        self.close()
        self.open_socket()
        return True

    def close(self):
        """
        close socket
        """
        self.s.shutdown(socket.SHUT_RDWR)
        self.s.close()
    
    def open_socket(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
            
            
            
            
            
    
    
