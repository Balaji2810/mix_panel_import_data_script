import json
import logging
import gzip
import bson
import os
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
import boto3
import configparser

from . import mixpanel_import

config = configparser.ConfigParser()
config.read("secret.ini")


def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=config["S3"]["access_key"],
        aws_secret_access_key=config["S3"]["secret_key"],
    )
    return session.client("s3")


def get_keys_from_prefix(client):
    keys_list = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=config["S3"]["bucket_name"], Prefix=config["S3"]["prefix"], Delimiter="/"
    ):
        keys = [content["Key"] for content in page.get("Contents")]
        keys_list.extend(keys)
    return keys_list


def get_content(client, key):
    data = client.get_object(Bucket=config["S3"]["bucket_name"], Key=key)
    contents = data["Body"].read()
    return bson.decode_all(contents.decode("utf-8"))


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


def start(state="local"):
    try:
        if state.lower() == "local":
            path = config["LOCAL"]["path"]
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
        elif state.lower == "s3":
            client = get_s3_client()
            for key in get_keys_from_prefix(client):
                collection_name = get_collection_name_from_file_name(key)
                if collection_name in MIXPANEL_IMPORT_STRUCTURE:
                    collection = get_data_from_gzip(key)
                    print(f"{len(collection)} documents in ", key)
                    count = 0
                    for document in collection:
                        count += 1
                        print(
                            f"--->{count}/{len(collection)} of documents imported to mixpanel",
                            end="\r",
                        )
                        mixpanel_import.import_data(
                            document, collection_name, f"S3 dump, {key}"
                        )
                    print()
                    print("-" * 50)
    except Exception as e:
        print(e)
