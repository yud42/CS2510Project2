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


if __name__ == "__main__":
    """
    threads = []
    for data_path, port in [("data/data_1", fs.StorageServerPortBase + 1),
                            ("data/data_2", fs.StorageServerPortBase + 2),
                            ("data/data_3", fs.StorageServerPortBase + 3)]:
        i_thread = threading.Thread(target=run_ss, args=(data_path, port))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)

    for t in threads:
        t.join()
    """
    servers = []
    for data_path, port in [("data/data_1", fs.StorageServerPortBase + 1),
                            ("data/data_2", fs.StorageServerPortBase + 2),
                            ("data/data_3", fs.StorageServerPortBase + 3)]:
        ss = fs.StorageServer(data_path, port)
        servers.append(ss)

    threads = []
    for ss in servers:
        i_thread = threading.Thread(target=run_ss, args=(ss,))
        i_thread.daemon = True
        i_thread.start()
        threads.append(i_thread)

    time.sleep(5)

    s = servers[0]
    s.stop()
    
    try:
        time.sleep(1000)
    except KeyboardInterrupt:
        for ss in servers:
            ss.stop()





