import json
import logging
import gzip
import bson
import os
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
import mixpanel_import
from datetime import datetime


def get_data_from_gzip(filename):
    with gzip.open(filename, "rb") as file:
        bson_content = file.read()
    return bson.decode_all(bson_content)


def get_all_files_from_directory(path, endswith=".gz"):
    dir_list = [
        [
            os.path.abspath(os.path.join(dirpath, file))
            for file in filenames
            if file.endswith(endswith)
        ]
        for dirpath, _, filenames in os.walk(path)
    ]
    if dir_list == [] or dir_list == [[]]:
        raise Exception(f"No gzip file found in the path {path}")
    return dir_list[0]


def get_collection_name_from_file_name(file_path):
    filename = os.path.basename(file_path)
    collection_name = filename.split("/")[-1].split("_")[0]
    return collection_name.lower()


def start():
    try:
        path = input("Enter the path where the gzip file present : ")
        for file in get_all_files_from_directory(path):
            collection_name = get_collection_name_from_file_name(file)
            if collection_name in MIXPANEL_IMPORT_STRUCTURE:

                collection = get_data_from_gzip(file)
                print(f"{len(collection)} documents in ", file)
                count = 0
                for document in collection:
                    count += 1
                    print(
                        f"--->{count}/{len(collection)} of documents imported to mixpanel",
                        end="\r",
                    )
                    mixpanel_import.import_data(
                        document, collection_name, f"Mongodb dump, {file}"
                    )
                print()
                print("-" * 50)
    except Exception as e:
        print(e)
