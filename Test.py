#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test.py
Program running to evaluate the file system
"""

import FileSystem as fs
import utils
import threading
import time

def launch_directory(ds):
    """
    launch directory server
    :param ds: a directory server object we want to launch
    """
    try:
        ds.run()
    except KeyboardInterrupt:
        ds.stop()

def run_directory(storage_nodes):
    """
    run a pair of directory server based on configs
    :param storage_nodes: a list of storage nodes configs ((addr, port), status) belong to the system
    :return directory_servers: a list of directory server objects for later manipulation
    :return threads: a list of threads launched by the function (running directory servers)
    """
    directory_servers = []
    threads = []
    
    ds = fs.DirectoryServer(fs.DirectoryServerIP, fs.DirectoryServerPortBase + 1, storage_nodes)
    ds.primary = True
    directory_servers.append(ds)
    
    ds_bp = fs.DirectoryServer(fs.DirectoryServerIP, fs.DirectoryServerPortBase + 2, storage_nodes)
    ds.primary = False
    directory_servers.append(ds_bp)
    
    for ds in directory_servers:
        i_thread = threading.Thread(target=launch_directory, args=(ds,))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)
    
    return directory_servers, threads
        
        
def launch_storage(ss):
    """
    launch storage node
    :param ss: a storage server object we want to launch
    """
    try:
        ss.run()
    except KeyboardInterrupt:
        ss.stop()

def run_storage(configs):
    """
    run storage nodes
    :param configs: a list of (data_path, port) configs for storage servers
    :return storage_servers: a list of storage server objects for later manipulation
    :return threads: a list of threads launched by the function (running storage servers)
    """
    storage_servers = []
    threads = []
    
    for path, port in configs:
        ss = fs.StorageServer(path, port)
        storage_servers.append(ss)
        
    for ss in storage_servers:
        i_thread = threading.Thread(target=launch_storage, args=(ss,))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)
    
    return storage_servers, threads
    
    
def launch_client(cl, file = None, filename = None):
    """
    launch client for different uses
    """
    
    
def run_client():
    """
    run client
    """
    
    
storage_nodes = [((fs.StorageServerIP, fs.StorageServerPortBase + 1), 1),
                 ((fs.StorageServerIP, fs.StorageServerPortBase + 2), 1),
                 ((fs.StorageServerIP, fs.StorageServerPortBase + 3), 1)]

configs = [("data/data_1", fs.StorageServerPortBase + 1),
           ("data/data_2", fs.StorageServerPortBase + 2),
           ("data/data_3", fs.StorageServerPortBase + 3)]