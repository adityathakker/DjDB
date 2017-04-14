import ConfigParser
import logging
import os
import pickle
import pprint

pp = pprint.PrettyPrinter(indent=4)

logger = logging.getLogger('djdb')

class Database:

    def __init__(self, client_id):
        self.client_id = client_id
        config = ConfigParser.ConfigParser()
        config.read('djdb.cfg')
        _, self.djdb_dir = config.items("djdb.dir")[0]
        self.djdb_dir = os.path.expanduser(self.djdb_dir)
        self.clients_count = 0
        for k, v in config.items("djdb.clients"):
            self.clients_count += 1

    def create_database(self, name):
        if not os.path.exists(self.djdb_dir + "/" + name):
            os.makedirs(self.djdb_dir + "/" + name)
            with open(self.djdb_dir + "/meta/" + name + ".pkl", "wb") as meta:
                info = dict()
                info["tables"] = dict()
                pickle.dump(info, meta)

        else:
            logger.debug("database already exists")
            print "database already exists"

    def create_table(self, db_name, table_name):
        if not os.path.exists(self.djdb_dir + "/" + db_name + "/" + table_name):
            for i in range(0, self.clients_count):
                with open(self.djdb_dir + "/" + db_name + "/" + table_name + str(i) + ".pkl", "wb") as table_file:
                    pickle.dump(dict(), table_file)

            info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
            info["tables"][table_name] = dict()
            info["tables"][table_name]["next_key"] = 0
            info["tables"][table_name]["locked"] = False
            with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
        else:
            logger.debug("table already exists")
            print "table already exists"

    def delete_table(self, db_name, table_name):
        for i in range(0, self.clients_count):
            if os.path.exists(self.djdb_dir + "/" + db_name + "/" + table_name + str(i) + ".pkl"):
                os.remove(self.djdb_dir + "/" + db_name + "/" + table_name + str(i) + ".pkl")
            else:
                logger.debug("table does not exists")
                print "table does not exists"
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
        info["tables"].pop(table_name, None)
        with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
            pickle.dump(info, meta)



    def delete_database(self, db_name):
        if os.path.exists(self.djdb_dir + "/" + db_name):
            import shutil
            shutil.rmtree(self.djdb_dir + "/" + db_name)
            os.remove(self.djdb_dir + "/meta/" + db_name + ".pkl")
        else:
            logger.debug("database does not exists")
            print "database does not exists"

    def insert_document(self, db_name, table_name, data):
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))

        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] is "w":

            table_no = info["tables"][table_name]["next_key"] % self.clients_count
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) + ".pkl", "rb"))
            document[info["tables"][table_name]["next_key"]] = data

            with open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) + ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)

            info["tables"][table_name]["next_key"] += 1
            with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
        else:
            print "you do not have permission. table is locked"

    def update_document(self, db_name, table_name, key, data):
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))

        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] is "w":

            table_no = key % self.clients_count

            document = pickle.load(open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) + ".pkl", "rb"))
            document[key] = data

            with open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) +  ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)

            info["tables"][table_name]["next_key"] += 1
            with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
        else:
            print "you do not have permission. table is locked"

    def delete_document(self, db_name, table_name, key):
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] is "w":

            table_no = key % self.clients_count
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) +  ".pkl", "rb"))
            if key in document:
                del document[key]
            else:
                print "no such key exists"

            with open(self.djdb_dir + "/" + db_name + "/" + table_name + str(table_no) + ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)
        else:
            print "you do not have permission. table is locked"

    def select_document(self, db_name, table_name):
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] is "r":
            document = dict()
            for i in range(self.clients_count):
                temp = pickle.load(open(self.djdb_dir + "/" + db_name + "/" + table_name + str(i) + ".pkl", "rb"))
                document.update(temp)
            pp.pprint(document)
        else:
            print "you do not have permission. table is locked"


    def lock_table(self, db_name, table_name, mode):
        if mode is "r" or mode is "w":
            info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
            info["tables"][table_name]["locked"] = True
            info["tables"][table_name]["locked_by"] = self.client_id
            info["tables"][table_name]["lock_type"] = mode
            with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
        else:
            print "mode is incorrect"

    def unlock_table(self, db_name, table_name):
        info = pickle.load(open(self.djdb_dir + "/meta/" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"]:
            info["tables"][table_name]["locked"] = False
            del info["tables"][table_name]["locked_by"]
            del info["tables"][table_name]["lock_type"]
            with open(self.djdb_dir + "/meta/" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
        else:
            print "table is already unlocked"





