# -*- coding: utf-8 -*-

"""
This script is for running the clients
"""
import FileSystem as fs
import utils
import time


if __name__ == "__main__":
    client = fs.Clients("data/client_1", fs.DirectoryServerPortBase + 1)
    file_list = client.get_FileList(isDir=True)
    if file_list:
        print("File list: {0}".format(file_list))
    else:
        print("File list: ")
        print(file_list)
    filename = "data/client_1/6sucess.txt"
    file = utils.obtain(filename)
    client.connect()
    
    filename_s = "6sucess.txt"
    client.addFile(filename_s, file)
    time.sleep(3)
    file_list = client.get_FileList(isDir=True)
    print("File list: {}".format(file_list))
    file_list = client.get_FileList(isDir=False)
    print("File list: {}".format(file_list))
    time.sleep(9)
    client.connect()
    file = client.readFile(filename_s)
    print(file)
