from pymongo import MongoClient, errors
import os
import json


MONGOPASS = os.getenv('MONGOPASS')
DB_NAME = 'qnd8mu'  
COLLECTION_NAME = 'project'  


uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"


client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)


db = client[DB_NAME]
collection = db[COLLECTION_NAME]


path = "data"


def import_json_file(file_path):
    try:
        with open(file_path) as file:
            file_data = json.load(file)
        
        if isinstance(file_data, list):
            collection.insert_many(file_data)
        else:
            collection.insert_one(file_data)
        return True
    except Exception as e:
        print(f"Error importing file {file_path}: {e}")
        return False


def main():
    successful_imports = 0
    failed_imports = 0
    corrupted_files = 0

    for (root, dirs, files) in os.walk(path):
        for f in files:
            file_path = os.path.join(root, f)
            if import_json_file(file_path):
                successful_imports += 1
            else:
                failed_imports += 1
                if not os.path.getsize(file_path):
                    corrupted_files += 1


    with open('count.txt', 'w') as count_file:
        count_file.write(f"Complete documents imported: {successful_imports}\n")
        count_file.write(f"Complete documents not imported: {failed_imports}\n")
        count_file.write(f"Corrupted documents: {corrupted_files}\n")

if __name__ == "__main__":
    main()