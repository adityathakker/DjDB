from database import Database
import ConfigParser
import os
import utils
import pickle
import pprint
import time

pp = pprint.PrettyPrinter(indent=4)

CLIENT_ID = 1

config = ConfigParser.ConfigParser()
config.read('djdb.cfg')

_, djdb_dir = config.items("djdb.dir")[0]
djdb_dir = os.path.expanduser(djdb_dir)

# utils.update_latency(djdb_dir)

db = Database(CLIENT_ID)
# db.create_database("temp")
# db.create_table("temp", "temp_table")
# db.delete_table("temp", "temp_table")
# db.delete_database("temp")

# db.lock_table("temp", "temp_table", "w", wait=True)
# db.insert_document("temp", "temp_table", {"name": "Aditya", "age": 20})
# db.insert_document("temp", "temp_table", {"name": "Adityaa", "age": 200})
# db.insert_document("temp", "temp_table", {"name": "Adityaaa", "age": 2000})
# db.insert_document("temp", "temp_table", {"name": "Pranit", "age": 2005})
# db.unlock_table("temp", "temp_table")
# print("3 Records Inserted")
# db.delete_document("temp", "temp_table", 1)

# db.lock_table("temp", "temp_table", "r")
# db.select_document("temp", "temp_table", columns=["name", "age"])
# db.unlock_table("temp", "temp_table")

# db.detect_deadlock()


# db.create_database("temp2")
# db.create_table("temp2", "temp_table")
# db.delete_table("temp", "temp_table")
# db.delete_database("temp")

# db.lock_table("temp2", "temp_table", "w", wait=False)
# db.insert_document("temp2", "temp_table", {"name": "Zditya", "age": 20})
# db.insert_document("temp2", "temp_table", {"name": "Zdityaa", "age": 200})
# db.insert_document("temp2", "temp_table", {"name": "Zdityaaa", "age": 2000})
# db.insert_document("temp2", "temp_table", {"name": "Zranit", "age": 2005})
# db.unlock_table("temp2", "temp_table")
# print("3 Records Inserted")
# db.delete_document("temp", "temp_table", 1)


# db.lock_table("temp2", "temp_table", "r")
# pp.pprint(db.select_document("temp2", "temp_table"))
# db.unlock_table("temp2", "temp_table")


# db.lock_table("temp", "temp_table", "r")
# pp.pprint(db.select_document("temp", "temp_table"))
# db.unlock_table("temp", "temp_table")


start_time = time.time()
# first apply join then condition
db.lock_table("temp", "temp_table", "r")
db.lock_table("temp2", "temp_table", "r")
pp.pprint(db.apply_condition_or(db.join(db.select_document("temp", "temp_table", columns=["name", "age"]), db.select_document("temp2", "temp_table", columns=["name", "age"])), conditions=[{"p1": "t1.age", "p2": 25, "op": "<"}, {"p1": "t2.age", "p2": 25, "op": "<"}]))
db.unlock_table("temp", "temp_table")
db.unlock_table("temp2", "temp_table")
print(time.time() - start_time)

start_time = time.time()
# first apply condition then join
db.lock_table("temp", "temp_table", "r")
db.lock_table("temp2", "temp_table", "r")
pp.pprint(db.join(db.apply_condition_or(db.select_document("temp", "temp_table", columns=["name", "age"]), conditions=[{"p1": "age", "p2": 25, "op": "<"}]), db.apply_condition_or(db.select_document("temp2", "temp_table", columns=["name", "age"]), conditions=[{"p1": "age", "p2": 25, "op": "<"}])))
db.unlock_table("temp", "temp_table")
db.unlock_table("temp2", "temp_table")
print(time.time() - start_time)


# db.detect_deadlock()

