import logging
import socket
import errno
import xmlrpclib

logger = logging.getLogger('djdb')

def safe_rpc(fn):
    def safe_fn(*args):
        try:
            result = fn(*args)
            if result is None:
                result = "success"

            return result
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED or e.errno == errno.EHOSTUNREACH:
                logger.critical("Problem connecting to rpc - no rpc server running. function: %s", fn.func_name)
                return None
            else:
                raise
    return safe_fn

@safe_rpc
def pull_file(dest_ip, dest_port, filename, source_uname, source_ip):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    rpc_connect.pull_file(filename, source_uname, source_ip)

@safe_rpc
def req_push_file(dest_ip, dest_port, filedata, source_uname, source_ip, source_port):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    return rpc_connect.req_push_file(filedata, source_uname, source_ip, source_port)

@safe_rpc
def ack_push_file(dest_ip, dest_port, server_filename, source_uname, source_ip, source_port):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    return rpc_connect.ack_push_file(server_filename, source_uname, source_ip, source_port)

@safe_rpc
def mark_presence(dest_ip, dest_port, source_ip, source_port):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    logger.debug("rpc call to mark available")
    logger.debug("available methods on rpc server %s", rpc_connect.system.listMethods())
    rpc_connect.mark_presence(source_ip, source_port)

@safe_rpc
def get_client_public_key(dest_ip, dest_port):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    return  rpc_connect.get_public_key()

def find_available(dest_ip, dest_port):
    rpc_connect = xmlrpclib.ServerProxy("http://%s:%s/"% (dest_ip, dest_port), allow_none = True)
    try:
        rpc_connect.system.listMethods()
        return True
    except socket.error as e:
        if e.errno == errno.ECONNREFUSED or e.errno == errno.EHOSTUNREACH:
            return False
        else:
            raise

