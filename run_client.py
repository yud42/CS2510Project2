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
import argparse
import random
from collections import defaultdict
import csv

task_log = {'add':[],'get':[]}
def launch_client(response_record, data_path, M):
    """
    launch client for different uses
    :param data_path: directory of the client
    :param M: file size in storage node
    """ 
    client = fs.Clients(data_path, fs.DirectoryServerPortBase + 1)              
    #CONNECT TEST: get location of storage nodes    
    client.connect()
    
    start = time.time() 
    
    fl = utils.get_file_list(data_path)
    #ADDFILE TEST: add local files to file system
    rn = random.randint(0,len(fl)-1)
    filename = fl[rn]
    task_log['add'].append(filename)
    print("Client on {0} adding file: {1}".format(data_path, filename))
    path = data_path + filename
    file = utils.obtain(path)
    print(file)
    client.addFile(filename, file)
    
    
    #GET_FILE_LIST test: get file list from directory and storage nodes
    file_list_dir = client.get_FileList(isDir=True)
    if len(file_list_dir)>M:
        file_list_dir = file_list_dir[:M]
    print("File list from directory server: {}".format(file_list_dir))
    

    if len(file_list_dir)==0 : return
#    tasks = [filename not in fl for filename in file_list_dir]
    tasks = file_list_dir
    rn = random.randint(0,len(tasks)-1)
    task = tasks[rn]
    task_log['get'].append(task)
    print("Client on {0} downloading: {1}".format(data_path, task))
    client.readFile(task)    
    end = time.time()
    response_time = end-start
    print("\nRequests in {0} of finished in {1:4f} second\n".format(data_path, response_time))
    accu, count = response_record[data_path]
    accu += response_time
    count += 3
    response_record[data_path] = accu,count

    
def run_client(response_record,datapath, F, M, N):
    """
    run client
    :param datapath: client data directory
    :param F: frequency to launch a new client running test tasks
    :return M: file size
    :return threads: on running client tasks
    """
    threads = []
    period = 1/F
    
    for i in range(N):
        print("Launching {} task".format(i))
        i_thread = threading.Timer(i*period, launch_client, args=(response_record,datapath,M))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)                                  

    return threads                                                                     
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='File system Evaluation Program')
    parser.add_argument('-M', '--filesize',type=str, default = '3', help = 'number of files initialized in a client')
    parser.add_argument('-N', '--requestsize',type=str, default = '1', help = 'number of readfile one client triggered')
    parser.add_argument('-F', '--frequency',type=str, default = '1', help = 'frequency of tasks triggered')
    
    args = parser.parse_args()
    M = int(args.filesize)
    N = int(args.requestsize)
    F = float(args.frequency)

    response_record = defaultdict(list)
            
    response_record = defaultdict(list)
    response_record["data/client_1/"] = [0,0]
    fs.reset_stats()
    
    
    client_threads = run_client(response_record, "data/client_1/", F, M, N)
    
    for t in client_threads:
        t.join()
    
    print('='*21 + 'statistics' + '='*21)
    
    fn='stats_response_time.csv'
    
    print("Adding tasks: {}".format(task_log['add']))
    print("Download tasks: {}".format(task_log['get']))
    
    
    accu,count = response_record["data/client_1/"]
    print("Average respond time in {0} is {1:4f}".format("data/client_1/",accu/count))

    msg_count,bytes_count = fs.get_stats()
#    writer.writerow(["Message count",msg_count])
#    writer.writerow(["Byte count",bytes_count])
    print("Total messages sent: {}".format(msg_count))
    print("Total bytes sent: {}".format(bytes_count))

