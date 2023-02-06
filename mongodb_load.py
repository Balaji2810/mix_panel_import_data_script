import configparser
from mongoengine import connect, disconnect
import json
import logging
import mixpanel_import
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
import datetime
from bson.objectid import ObjectId
from mongoengine.queryset.visitor import Q

config = configparser.ConfigParser()
config.read("secret.ini")


# def get_last_entry():
#     try:
#         with open("mixpanel_last_entry.json", "r") as openfile:
#             json_object = json.load(openfile)
#         return json_object
#     except:
#         return {"Payment": 0, "Transaction": 0}


# def set_last_entry(name, last_entry):
#     json_object = get_last_entry()
#     json_object[name] = last_entry
#     json_object = json.dumps(json_object, indent=4)
#     with open("mixpanel_last_entry.json", "w") as outfile:
#         outfile.write(json_object)


# def fetch_and_upload_payments(skip=0):
#     for count, payment in enumerate(Payment.objects.skip(skip)):
#         collection_name = "payments"
#         document = payment.to_mongo().to_dict()
#         document["_id"] = str(document["_id"])
#         mixpanel_import.import_data(document, collection_name, "Mongodb, Payments")
#         set_last_entry("Payment", count + skip + 1)


# def fetch_and_upload_transactions(skip=0):
#     for count, transaction in enumerate(Transaction.objects.skip(skip)):
#         collection_name = "transactions"
#         document = transaction.to_mongo().to_dict()
#         document["_id"] = str(document["_id"])
#         mixpanel_import.import_data(document, collection_name, "Mongodb, transactions")
#         set_last_entry("Transaction", count + skip + 1)


def fetch_and_upload(collection_name, start_datetime, end_datetime):
    print(MIXPANEL_IMPORT_STRUCTURE[collection_name]["collection"])
    collection = MIXPANEL_IMPORT_STRUCTURE[collection_name]["collection"].objects(
        # Q(id__gt=get_object_id(start_datetime))
        # & Q(
        #     id__lt=get_object_id(
        #         end_datetime
        #         + datetime.timedelta(days=1)  # end date is included in the range
        #     )
        # )
    )
    doc_count = collection.count()
    print(
        "Collection :",
        collection_name,
        "Documents :",
        doc_count,
        len(collection),
    )
    count = 0
    for document in collection:
        count += 1
        print(
            f"--->{count}/{doc_count} of documents imported to mixpanel",
            end="\r",
        )

        document = document.to_mongo().to_dict()
        document["_id"] = str(document["_id"])

        mixpanel_import.import_data(
            document, collection_name, f"Mongodb, {collection_name}"
        )

    print()
    print("-" * 50)


def timezone_offset(timezone):
    hours, mins = map(int, timezone[1:].split(":"))
    return -eval(timezone[0] + f" datetime.timedelta(hours={hours},minutes={mins})")


def get_object_id(dt):
    return ObjectId.from_datetime(dt)


def start(start_datetime, end_datetime, offset):
    start_datetime = start_datetime + timezone_offset(offset)
    end_datetime = end_datetime + timezone_offset(offset)
    try:
        connect(
            db=config["MONGODB"]["db"],
            host=config["MONGODB"]["uri"],
        )

        for collection_name in MIXPANEL_IMPORT_STRUCTURE:
            fetch_and_upload(collection_name, start_datetime, end_datetime)
    except Exception as e:
        print(e)
        logging.error(str(e))

    finally:
        disconnect()
