"""
Utility functions for p2p network
"""
import p2p
import os
import socket
import json


def decode_query_message(query_message):
    """
    Decode query message for file location.
    :param query_message:
    :return:
    """
    return query_message


def encode_query_message(file_name):
    query_message = p2p.QUERY_HEADER + file_name
    return query_message


def encode_location_message(peer_ids):
    """
    Encode return message for file location.
    :param peer_ids: a set of peers which contain the requested file.
    :param file_name
    :return:
    """
    return p2p.LOCATION_HEADER + json.dumps(list(peer_ids))


def decode_location_message(location_message):
    return json.loads(location_message)

def encode_list_message(file_list):
    return p2p.LIST_HEADER + json.dumps(file_list)

def decode_list_message(list_message):
    return json.loads(list_message)

def encode_update_message(file_name, file):
    message_dict = {'file_name':file_name, 'file':file}
    return p2p.UPDATE_HEADER + json.dumps(message_dict)

def decode_update_message(update_message):
    message_dict = json.loads(update_message)
    return message_dict['file_name'],message_dict['file']



def obtain(filename):
    """
    obtain data by request 
    :param filename: path to the data
    """
    try:
        with open(filename,'rb') as f:
            return f.read()
    except:
        print("Something went wrong during obtaining file")
        
    
    
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
    if ord(filename[0]) < p2p.S1:
        # connect to index server A
        return p2p.INDEX_PORT_A
    elif ord(filename[0]) < p2p.S2:
        # connect to index server B
        return p2p.INDEX_PORT_B
    else:
        # connect to index server C
        return p2p.INDEX_PORT_C
     
def get_list(filepath):
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
            
    