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

    def create_database(self, name):
        if not os.path.exists(self.djdb_dir + "/" + name + ".pkl"):
            with open(self.djdb_dir + "/meta_" + name + ".pkl", "wb") as meta:
                info = dict()
                info["tables"] = dict()
                pickle.dump(info, meta)
                meta.close()

            if not os.path.exists(self.djdb_dir + "/meta_global.pkl"):
                with open(self.djdb_dir + "/meta_global.pkl", "wb") as meta_global:
                    temp = dict()
                    temp["databases"] = dict()
                    pickle.dump(temp, meta_global)
                    meta_global.close()

            with open(self.djdb_dir + "/meta_global.pkl", "rb") as meta_global_file:
                meta_global = pickle.load(meta_global_file)
                if "databases" in meta_global:
                    meta_global["databases"][name] = dict()
                else:
                    meta_global["databases"] = dict()
                    meta_global["databases"][name] = dict()
                    meta_global_file.close()

            with open(self.djdb_dir + "/meta_global.pkl", "wb") as meta_global_file:
                pickle.dump(meta_global, meta_global_file)
        else:
            logger.debug("database already exists")
            print "database already exists"

    def create_table(self, db_name, table_name):
        if not os.path.exists(self.djdb_dir + "/" + db_name + "_" + table_name):
            with open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "wb") as table_file:
                pickle.dump(dict(), table_file)
                table_file.close()

            info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
            info["tables"][table_name] = dict()
            info["tables"][table_name]["next_key"] = 0
            info["tables"][table_name]["locked"] = False
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
                meta.close()
        else:
            logger.debug("table already exists")
            print "table already exists"

    def delete_table(self, db_name, table_name):
        if os.path.exists(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl"):
            os.remove(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl")
        else:
            logger.debug("table does not exists")
            print "table does not exists"

        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        info["tables"].pop(table_name, None)
        with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
            pickle.dump(info, meta)
            meta.close()

    def delete_database(self, db_name):
        if os.path.exists(self.djdb_dir + "/meta" + db_name + ".pkl"):
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb") as meta_db_file:
                meta_db = pickle.load(meta_db_file)
                for k in meta_db["tables"]:
                    os.remove(self.djdb_dir + "/" + db_name + "_" + k + ".pkl")
            os.remove(self.djdb_dir + "/meta_" + db_name + ".pkl")
        else:
            logger.debug("database does not exists")
            print "database does not exists"

    def insert_document(self, db_name, table_name, data):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))

        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] == "w":
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "rb"))
            document[info["tables"][table_name]["next_key"]] = data

            with open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)
                table_file.close()

            info["tables"][table_name]["next_key"] += 1
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
                meta.close()
        else:
            print "you do not have permission. you need to lock the table first"

    def update_document(self, db_name, table_name, key, data):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))

        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] == "w":
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "rb"))
            document[key] = data

            with open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)
                table_file.close()

            info["tables"][table_name]["next_key"] += 1
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
                meta.close()
        else:
            print "you do not have permission. you need to lock the table first"

    def delete_document(self, db_name, table_name, key):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] == "w":
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "_" + table_name +  ".pkl", "rb"))
            if key in document:
                del document[key]
            else:
                print "no such key exists"

            with open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "wb") as table_file:
                pickle.dump(document, table_file)
                table_file.close()
        else:
            print "you do not have permission. you need to lock the table first"

    def select_document(self, db_name, table_name):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] == "r":
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "rb"))
            pp.pprint(document)
        else:
            print "you do not have permission. you need to lock the table first"


    def lock_table(self, db_name, table_name, mode):
        if mode is "r" or mode is "w":
            info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
            info["tables"][table_name]["locked"] = True
            info["tables"][table_name]["locked_by"] = self.client_id
            info["tables"][table_name]["lock_type"] = mode
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
                meta.close()
        else:
            print "mode is incorrect"

    def unlock_table(self, db_name, table_name):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"]:
            info["tables"][table_name]["locked"] = False
            del info["tables"][table_name]["locked_by"]
            del info["tables"][table_name]["lock_type"]
            with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                pickle.dump(info, meta)
                meta.close()
        else:
            print "table is already unlocked"





