# -*- coding: utf-8 -*-

"""
This script is for running the directory servers
"""
import FileSystem as fs
import threading
import time


def run_ds(ds):
    ds.launch_bp_directory_server_thread()
    ds.run()


if __name__ == "__main__":
    storage_nodes = [((fs.StorageServerIP, fs.StorageServerPortBase + 1), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 2), 1),
                     ((fs.StorageServerIP, fs.StorageServerPortBase + 3), 1)]
    ds = fs.DirectoryServer(fs.DirectoryServerIP, fs.DirectoryServerPortBase + 1, storage_nodes)
    ds.primary = True
    i_thread = threading.Thread(target=run_ds, args=(ds,))
    i_thread.daemon = True
    i_thread.start()

    time.sleep(8)
    ds.stop()

    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        pass
