"""
Utility functions for FileSystem network
"""
import FileSystem
import os
import socket
import json



def encode_location_message(location):
    """
    Encode return message for file location.
    :param location: a set of peers which contain the requested file.
    :return:
    """
    return FileSystem.LOCATION_HEADER + json.dumps(location)


def decode_location_message(location_message):
    return json.loads(location_message)


def encode_list_message(file_list):
    return FileSystem.LIST_HEADER + json.dumps(list(file_list))


def decode_list_message(list_message):
    return json.loads(list_message)


def encode_update_message(file_name, file, addr):
    message_list = [file_name, file, addr]
    return FileSystem.DATA_HEADER + json.dumps(message_list) + FileSystem.DATA_TAIL


def decode_update_message(update_message):
    try:
        message_list = json.loads(update_message)
    except ValueError:
        print("error update message {}".format(update_message))

    return message_list[0],message_list[1],message_list[2]


def encode_request_message(file_name):
    return FileSystem.REQUEST_HEADER + file_name


def decode_request_message(request_message):
    return request_message


def encode_bp_sn_message(storage_nodes):
    return FileSystem.BP_STORAGE_HEADER + json.dumps(storage_nodes)


def decode_bp_sn_message(message):
    storage_nodes = json.loads(message)
    storage_nodes_2 = []
    for s in storage_nodes:
        status = s[1]
        addr, port = s[0]
        addr = addr.encode(FileSystem.COD)
        storage_nodes_2.append(((addr, port), status))
    return storage_nodes_2


def encode_bp_fl_message(file_list):
    return FileSystem.BP_FL_HEADER + json.dumps(list(file_list))


def decode_bp_fl_message(message):
    file_list = json.loads(message)
    return set(file_list)


def obtain(filename):
    """
    obtain data by request 
    :param filename: path to the data
    """
    try:
        with open(filename,'rt') as f:
            return f.read()
    except:
        print("Something went wrong during obtaining file: {}\n".format(filename))
        
    
    
def write_data(data,filename, mode):
    """
    write data by to file 
    :param data: data to write
    :param filename: path to the file
    """
    try:
        with open(filename,mode) as f:
            f.write(data)
    except:
        print("Something went wrong during writing file")
        
        

def get_live_peer(locations):
    """
    pickup closest peer among candidates
    :param locations: a candidate list consist of (address,port) pairs
    """
#    address = ""
#    port = 0
    for addr,p in locations:
        if os.system("ping -c 1 " + addr) == 0:
            return addr, p
    return False, False

def get_addr():
    """
    return address of current socket
    """
    hostname = socket.gethostname()    
    IPAddr = socket.gethostbyname(hostname)
    print("Host address: {}".format(IPAddr))
    return IPAddr


def get_index_server_port(filename):
    if ord(filename[0]) < FileSystem.S1:
        # connect to index server A
        return FileSystem.INDEX_PORT_A
    elif ord(filename[0]) < FileSystem.S2:
        # connect to index server B
        return FileSystem.INDEX_PORT_B
    else:
        # connect to index server C
        return FileSystem.INDEX_PORT_C
     
def get_file_list(filepath):
    datalist = os.listdir(filepath)
    for i in datalist:
        if i.startswith('.'):
            datalist.remove(i)
    return datalist

def generate_tasks(request_size, peers):
    """
    return task list such that each list is a task sequence generated by uniformly picked from peers
    :param request_size: number of requests each peers need to make
    :param peers: peer lists contains (datapath, port_number) pairs
    """
    
    res = []
    rest = len(peers) - 1
    dist_factor = request_size//rest
    dist_left = request_size%rest
    for peer in peers:
        tasks = []
        for other_peer in peers:
            if peer == other_peer: continue
            else:
                data,_ = other_peer
                datalist = os.listdir(data)
                for i in datalist:
                    if i.startswith('.'):
                        datalist.remove(i)
                if dist_left != 0 and len(tasks)==0:
                    for filename in datalist[:(dist_factor + dist_left)]:
                        if not filename.startswith('.'):
                            tasks.append(filename)
                else:
                    for filename in datalist[:(dist_factor)]:
                        if not filename.startswith('.'):
                            tasks.append(filename)                  
                    
        res.append(tasks)

    return res         
            
    