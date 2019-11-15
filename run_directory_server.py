# -*- coding: utf-8 -*-

"""
This script is for running the directory servers
"""
import FileSystem as fs

if __name__ == "__main__":
    storage_nodes = [(fs.StorageServerIP, fs.StorageServerPortBase), 1]
    ds = fs.DirectoryServer(fs.DirectoryServerIP, fs.DirectoryServerPortBase + 1, storage_nodes)
    ds.run()
