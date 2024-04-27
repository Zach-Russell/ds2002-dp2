import os
from db import *
import json

#path to json files
path = "../data"

#successful documents inserted
clean_successful = 0


##Note: this isn't being used now, realized that orphaned documents are in the corrupt file, not in successfully loaded files
#non corrupted documents unable to be inserted
clean_failed = 0

#corrupted files
corrupted = 0

for (root, dirs, fileArr) in os.walk(path):
    for f in fileArr:

        #Loading or Opening the json file
        with open(path + "/" + f) as file_descriptor:

            #attempt to load json data
            try:
                file_data = json.load(file_descriptor)

            #couldn't load, it's corrupted
            except json.JSONDecodeError as e:
                print("Corrupt File::", e)
                print("Corrupt FileName:", f)
                corrupted += 1

                #skip this iteration of the loop, don't want to insert corrupted data
                continue


        if isinstance(file_data, list):
            #attempt to insert many files
            try:
                collection.insert_many(file_data)

                #successful insertion
                clean_successful += len(file_data)
            
            #not corrupted, but couldn't insert
            except Exception as e:
                print("Failed Insertion::", e)
                clean_failed += len(file_data)

        else:
            #attempt to insert one file
            try:
                collection.insert_one(file_data)

                #successful insertion
                clean_successful += len(file_data)

            #not corrupted, but couldn't insert
            except Exception as e:
                print("Failed Insertion::", e)
                clean_failed += len(file_data)

#print number of documents inserted
print("Successfully inserted documents:", clean_successful)

#print number of corrupt files, can't count number of documents since wasn't able to be imported
print("Corrupt files:", corrupted)




