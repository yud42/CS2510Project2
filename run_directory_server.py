# -*- coding: utf-8 -*-

"""
This script is for running the directory servers
"""
import FileSystem as fs
import threading
import time
import argparse


def run_ds(ds):
    ds.launch_bp_directory_server_thread()
    ds.run()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='File system Evaluation Program')
    parser.add_argument('-T', '--down_time',type=str, default = '10', help = 'time from start the directory node was killed')
    
    args = parser.parse_args()
    T = int(args.down_time)
    
    T = 100
    
    fs.reset_stats()
    storage_nodes = [((fs.StorageServerIP, fs.StorageServerPortBase + 1), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 2), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 3), 1)]
    ds = fs.DirectoryServer(fs.DirectoryServerIP, fs.DirectoryServerPortBase + 1, storage_nodes)
    ds.primary = True
    i_thread = threading.Thread(target=run_ds, args=(ds,))
    i_thread.daemon = True
    i_thread.start()

    time.sleep(T)
    ds.stop()
    
    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        msg_count,bytes_count = fs.get_stats()

        print("Total messages sent: {}".format(msg_count))
        print("Total bytes sent: {}".format(bytes_count))

