from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import logging
import os
import re
import subprocess
import threading

logger = logging.getLogger('djdb')

class Handler(SimpleXMLRPCRequestHandler):
    def _dispatch(self, method, params):
        try:
            print self.server.funcs.items()
            return self.server.funcs[method](*params)
        except:
            import traceback
            traceback.print_exc()
            raise


class Node(object):

    def __init__(self, role , ip, port, uname, watch_dirs):
        self.role = role
        self.ip = ip
        self.port = port
        self.username = uname
        self.watch_dirs = watch_dirs

    @staticmethod
    def get_dest_path(filename, dest_uname):
        user_dir_pattern = re.compile("/home/[^ ]*?/")

        if re.search(user_dir_pattern, filename):
            destpath = user_dir_pattern.sub("/home/%s/" % dest_uname, filename)
        logger.debug("destpath %s", destpath)
        return destpath


    @staticmethod
    def push_file(filename, dest_uname, dest_ip):
        proc = subprocess.Popen(['scp -r', filename, "%s@%s:%s" % (dest_uname, dest_ip, Node.get_dest_path(filename, dest_uname))])
        return_status = proc.wait()
        logger.debug("returned status %s",return_status)

    def ensure_dir(self):
        for dir in self.watch_dirs:
            if not os.path.isdir(dir):
                os.makedirs(dir)

    def start_server(self):
        server = SimpleXMLRPCServer(("0.0.0.0", self.port), allow_none =True)
        server.register_instance(self)
        server.register_introspection_functions()
        rpc_thread = threading.Thread(target=server.serve_forever)
        rpc_thread.start()
        logger.debug("server functions on rpc %s", server.funcs.items())
        logger.info("Started RPC server thread. Listening on port %s..." , self.port)


    def start_sync_thread(self):
        sync_thread = threading.Thread(target=self.sync_files)
        sync_thread.start()
        logger.info("Thread 'syncfiles' started ")

    def activate(self):
        self.ensure_dir()
        self.start_sync_thread()
        self.start_server()

