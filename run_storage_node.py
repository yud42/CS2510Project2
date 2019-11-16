# -*- coding: utf-8 -*-

"""
This script is for running the storage nodes
"""
import FileSystem as fs
import threading
import time
import socket

def run_ss(ss):
    ss.run()

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

if __name__ == "__main__":
    fs.reset_stats()
    storage_configs = [("data/data_1", fs.StorageServerPortBase + 1),
                       ("data/data_2", fs.StorageServerPortBase + 2),
                       ("data/data_3", fs.StorageServerPortBase + 3)]
    servers, threads = run_storage(storage_configs)
    
#    time.sleep(5)
#
#    s = servers[0]
#    s.stop()
    
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        for ss in servers:
            ss.stop()
        
        msg_count,bytes_count = fs.get_stats()

        print("Total messages sent: {}".format(msg_count))
        print("Total bytes sent: {}".format(bytes_count))





