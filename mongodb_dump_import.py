import json
import logging
import gzip
import bson
import os
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
import main
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
                    obj = {
                        "distinct_id": document[
                            MIXPANEL_IMPORT_STRUCTURE[collection_name]["distinct_id"]
                        ],
                        "event_name": MIXPANEL_IMPORT_STRUCTURE[collection_name][
                            "event_name"
                        ],
                        "timestamp": document[
                            MIXPANEL_IMPORT_STRUCTURE[collection_name]["timestamp"]
                        ],
                        "props": {
                            **{
                                key["name"]: document[key["name"]]
                                if key["name"] in document
                                else key["default"]
                                for key in MIXPANEL_IMPORT_STRUCTURE[collection_name][
                                    "props"
                                ]
                            },
                            **{
                                "$insert_id": document[
                                    MIXPANEL_IMPORT_STRUCTURE[collection_name][
                                        "$insert_id"
                                    ]
                                ]
                            },
                        },
                    }
                    main.import_data(
                        obj["distinct_id"],
                        obj["event_name"],
                        obj["timestamp"],
                        obj["props"],
                    )
                    logging.info(
                        LOG_FORMAT.format(
                            **{
                                "from": f"Mongodb dump, {file}",
                                "data": json.dumps(obj, indent=4, default=str),
                                "time": datetime.now(),
                            }
                        )
                    )
    except Exception as e:
        print(e)
