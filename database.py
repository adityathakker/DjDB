import ConfigParser
import logging
import os
import pickle
import pprint
import time

pp = pprint.PrettyPrinter(indent=4)

logger = logging.getLogger('djdb')

class Database:

    def __init__(self, client_id):
        self.client_id = client_id
        self.config = ConfigParser.ConfigParser()
        self.config.read('djdb.cfg')
        _, self.djdb_dir = self.config.items("djdb.dir")[0]
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


    def lock_table(self, db_name, table_name, mode, wait=False):
        if mode is "r" or mode is "w":
            info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))

            if not info["tables"][table_name]["locked"]:
                info["tables"][table_name]["locked"] = True
                info["tables"][table_name]["locked_by"] = self.client_id
                info["tables"][table_name]["lock_type"] = mode

                if "waiting" not in info["tables"][table_name]:
                    info["tables"][table_name]["waiting"] = list()

                if self.client_id in info["tables"][table_name]["waiting"]:
                    del info["tables"][table_name]["waiting"][info["tables"][table_name]["waiting"].index(self.client_id)]

                with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                    pickle.dump(info, meta)
                    meta.close()
            else:
                if wait:
                    if "waiting" not in info["tables"][table_name]:
                        info["tables"][table_name]["waiting"] = list()
                    if self.client_id  not in info["tables"][table_name]["waiting"]:
                        info["tables"][table_name]["waiting"].append(self.client_id)
                    with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                        pickle.dump(info, meta)
                        meta.close()

                    time.sleep(5)
                    if self.detect_deadlock():
                        print "deadlock detected"
                        return
                    print "waiting"
                    self.lock_table(db_name, table_name, mode, wait=True)
                else:
                    print "table is already locked by client " + str(info["tables"][table_name]["locked_by"])
        else:
            print "mode is incorrect"


    def unlock_table(self, db_name, table_name):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"]:
            if info["tables"][table_name]["locked_by"] == self.client_id:
                info["tables"][table_name]["locked"] = False
                del info["tables"][table_name]["locked_by"]
                del info["tables"][table_name]["lock_type"]
                # del info["waiting"][table_name]["waiting"]
                with open(self.djdb_dir + "/meta_" + db_name + ".pkl", "wb") as meta:
                    pickle.dump(info, meta)
                    meta.close()
            else:
                print "table is locked by someone else. you cannot unlock it"
        else:
            print "table is already unlocked"

    def detect_deadlock(self):
        clients_dict = dict()
        for k, v in self.config.items("djdb.clients"):
            clients_dict[k] = list()

        with open(self.djdb_dir + "/meta_global.pkl", "rb") as global_meta_file:
            global_meta = pickle.load(global_meta_file)

            for db in global_meta["databases"]:
                with open(self.djdb_dir + "/meta_" + db + ".pkl", "rb") as db_meta_file:
                    db_meta = pickle.load(db_meta_file)

                    for table in db_meta["tables"]:
                        if db_meta["tables"][table]["locked"]:
                            print("client" + str(db_meta["tables"][table]["waiting"]))
                            for c in db_meta["tables"][table]["waiting"]:
                                clients_dict["client" + str(db_meta["tables"][table]["locked_by"])].append(c)

        return self.cyclic(clients_dict)

    def cyclic(self,g):
        path = set()
        visited = set()

        def visit(vertex):
            if vertex in visited:
                return False
            visited.add(vertex)
            path.add(vertex)
            for neighbour in g.get(vertex, ()):
                if neighbour in path or visit(neighbour):
                    return True
            path.remove(vertex)
            return False

        return any(visit(v) for v in g)

    def select_document(self, db_name, table_name, columns=None, conditions=None):
        info = pickle.load(open(self.djdb_dir + "/meta_" + db_name + ".pkl", "rb"))
        if info["tables"][table_name]["locked"] and info["tables"][table_name]["locked_by"] == self.client_id and info["tables"][table_name]["lock_type"] == "r":
            document = pickle.load(open(self.djdb_dir + "/" + db_name + "_" + table_name + ".pkl", "rb"))
            if not columns:
                return self.apply_condition_or(document, conditions)
            else:
                new_doc = dict()
                for k in document:
                    new_doc[k] = dict()
                    for kk in document[k]:
                        if kk in columns:
                            new_doc[k][kk] = document[k][kk]
                return self.apply_condition_or(new_doc, conditions)

        else:
            print "you do not have permission. you need to lock the table first"

    def join(self, table1, table2, key=None, columns=None):
        # time.sleep(3)
        joined = dict()
        i = 0
        if not key:
            for k in table1:
                for kk in table2:
                    if columns:
                        if kk in columns and k in columns:
                            joined[i] = table1[k].update(table2[kk])
                            i += 1
                    else:
                        joined[i] = dict()
                        for k1 in table1[k]:
                            joined[i]["t1." + k1] = table1[k][k1]

                        for k2 in table2[kk]:
                            joined[i]["t2." + k2] = table2[kk][k2]

                        i += 1
            return joined

    def apply_condition_or(self, data, conditions=None):
        if not conditions:
            return data
        else:
            # pp.pprint(data)
            new_data = dict()
            for condition in conditions:
                # print "condition: " + str(condition)
                for k in data:
                    # print "K: " + str(k)
                    if condition["p1"] in data[k] and condition["op"] in ["<", "<=", ">=", ">", "=", "!="]:
                        if isinstance(condition["p2"], (int, float)) and isinstance(data[k][condition["p1"]], (int, float)):
                            if condition["op"] == "<":
                                if data[k][condition["p1"]] < condition["p2"]:
                                    new_data[k] = data[k]
                            elif condition["op"] == "<=":
                                if data[k][condition["p1"]] <= condition["p2"]:
                                    new_data[k] = data[k]
                            elif condition["op"] == ">":
                                if data[k][condition["p1"]] > condition["p2"]:
                                    new_data[k] = data[k]
                            elif condition["op"] == ">=":
                                if data[k][condition["p1"]] >= condition["p2"]:
                                    new_data[k] = data[k]
                            elif condition["op"] == "=":
                                if data[k][condition["p1"]] == condition["p2"]:
                                    new_data[k] = data[k]
                            elif condition["op"] == "!=":
                                if not data[k][condition["p1"]] == condition["p2"]:
                                    new_data[k] = data[k]
                            else:
                                print "ppopat"
                                return None

                        else:
                            if condition["p2"] in data[k]:
                                if isinstance(data[k][condition["p2"]], (int, float)) and isinstance(data[k][condition["p1"]], (int, float)):
                                    if condition["op"] == "<":
                                        if data[k][condition["p1"]] < data[k][condition["p2"]]:
                                            new_data[k] = data[k]
                                    elif condition["op"] == "<=":
                                        if data[k][condition["p1"]] <= data[k][condition["p2"]]:
                                            new_data[k] = data[k]

                                    elif condition["op"] == ">":
                                        if data[k][condition["p1"]] > data[k][condition["p2"]]:
                                            new_data[k] = data[k]
                                    elif condition["op"] == ">=":
                                        if data[k][condition["p1"]] >= data[k][condition["p2"]]:
                                            new_data[k] = data[k]
                                    elif condition["op"] == "=":
                                        if data[k][condition["p1"]] == data[k][condition["p2"]]:
                                            new_data[k] = data[k]
                                    elif condition["op"] == "!=":
                                        if not data[k][condition["p1"]] == data[k][condition["p2"]]:
                                            new_data[k] = data[k]
                                    else:
                                        print "ppopat"
                                        return None

                                else:
                                    print "incorrent type"
                                    return None
                            else:
                                print "incorrect column name"
                                return None
                    else:
                        print "Invalid Format"
                        return None

            return new_data









