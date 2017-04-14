from database import Database
import ConfigParser
import os
import utils
import pickle

CLIENT_ID = 2

config = ConfigParser.ConfigParser()
config.read('djdb.cfg')

_, djdb_dir = config.items("djdb.dir")[0]
djdb_dir = os.path.expanduser(djdb_dir)

# utils.update_latency(djdb_dir)

db = Database(CLIENT_ID)
db.create_database("temp")
db.create_table("temp", "temp_table")
db.delete_table("temp", "temp_table")
db.delete_database("temp")

# db.insert_document("temp", "temp_table", {"name": "Aditya", "age": 20})
# db.insert_document("temp", "temp_table", {"name": "Adityaa", "age": 200})
# db.insert_document("temp", "temp_table", {"name": "Adityaaa", "age": 2000})

# db.delete_document("temp", "temp_table", 1)


# db.select_document("temp", "temp_table")
