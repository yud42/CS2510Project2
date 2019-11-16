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
    
    
def launch_client(cl, file_num, task_num):
    """
    launch client for different uses
    :param cl: client object to launch tasks
    :paran file_num: number of files to add to system
    :param task_num: number of files to download
    """ 
    fl = utils.get_file_list(cl.data_path)
    if len(fl) > file_num:
        fl = fl[:file_num]
        
    #CONNECT TEST: get location of storage nodes    
    cl.connect()
    #interval for connect totally finished
    time.sleep(3)
    
    #ADDFILE TEST: add local files to file system
    for filename in fl:
        path = cl.data_path + filename
        file = utils.obtain(path)
        cl.addFile(filename, file)
    #interval for add files totally finished
    time.sleep(5)
    
    #GET_FILE_LIST test: get file list from directory and storage nodes
    file_list_dir = cl.get_FileList(isDir=True)
    print("File list from directory server: {}".format(file_list_dir))
    file_list_s = cl.get_FileList(isDir=False)
    print("File list from directory server: {}".format(file_list_s))
    #interval for add files totally finished
    time.sleep(5)
    
    tasks = [filename not in fl for filename in file_list_dir]
    if len(tasks) > task_num:
        tasks = tasks[:task_num]
        
    print("Client on {0} tasks: {1}".format(cl.data_path, tasks))
    
    for task in tasks:
        cl.readFile(task)
    
    
def run_client(configs, frequency, filenum, tasknum, delay=0):
    """
    run client
    :param configs: list of client configs need to be launched, a list of data directory
    :param frequency: frequency to launch a new client running test tasks
    :return clients: clients opened
    :return threads: on running client tasks
    """
    clients = []
    threads = []
    period = 1/frequency
    for directory in configs:
        client = fs.Clients(configs, fs.DirectoryServerPortBase + 1)     
        clients.append(client)
    
    for i,cl in enumerate(clients):
        i_thread = threading.Timer(i*period, target=launch_storage, args=(cl,filenum,tasknum))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)                                  
                                

    return clients, threads                                                                     
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Peer-to-Peer system Evaluation Program')
    parser.add_argument('-M', '--filesize',type=str, default = '3', help = 'number of files initialized in a client')
    parser.add_argument('-N', '--requestsize',type=str, default = '1', help = 'number of readfile one client triggered')
    parser.add_argument('-F', '--frequency',type=str, default = '1', help = 'frequency of tasks triggered')
    
    args = parser.parse_args()
    
    storage_nodes = [((fs.StorageServerIP, fs.StorageServerPortBase + 1), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 2), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 3), 1)]
    
    storage_configs = [("data/data_1", fs.StorageServerPortBase + 1),
                       ("data/data_2", fs.StorageServerPortBase + 2),
                       ("data/data_3", fs.StorageServerPortBase + 3)]
    
    client_configs = ["data/client_1", "data/client_2"]

