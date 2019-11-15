# -*- coding: utf-8 -*-

"""
This script is for running the storage nodes
"""
import FileSystem as fs

if __name__ == "__main__":
    ss = fs.StorageServer("data/data_1", fs.StorageServerPortBase + 1)
    ss.run()