import os
from db import *
import json

### Traverse through files

path = "../data"

clean_successful = 0
clean_failed = 0
corrupted = 0

for (root, dirs, file) in os.walk(path):
    for f in file:
        print(f)
        #Insert into mongodb
        # assuming you have defined a connection to your db and collection already:


        #Loading or Opening the json file
        with open(path + "/" + f) as file_descriptor:
            file_data = json.load(file_descriptor)
            
        # Inserting the loaded data in the collection
        # if JSON contains data more than one entry
        # insert_many is used else insert_one is used
        if isinstance(file_data, list):
            collection.insert_many(file_data)  
        else:
            collection.insert_one(file_data)






