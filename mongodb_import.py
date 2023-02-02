import configparser
from models import Payment, Transaction
from mongoengine import connect, disconnect
import json
import logging
import main
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
from datetime import datetime

config = configparser.ConfigParser()
config.read("config.ini")


def get_last_entry():
    try:
        with open("mixpanel_last_entry.json", "r") as openfile:
            json_object = json.load(openfile)
        return json_object
    except:
        return {"Payment": 0, "Transaction": 0}


def set_last_entry(name, last_entry):
    json_object = get_last_entry()
    json_object[name] = last_entry
    json_object = json.dumps(json_object, indent=4)
    with open("mixpanel_last_entry.json", "w") as outfile:
        outfile.write(json_object)


def fetch_and_upload_payments(skip=0):
    for count, payment in enumerate(Payment.objects.skip(skip)):
        collection_name = "payments"
        document = payment.to_mongo().to_dict()
        document["_id"] = str(document["_id"])
        obj = {
            "distinct_id": document[
                MIXPANEL_IMPORT_STRUCTURE[collection_name]["distinct_id"]
            ],
            "event_name": MIXPANEL_IMPORT_STRUCTURE[collection_name]["event_name"],
            "timestamp": document[
                MIXPANEL_IMPORT_STRUCTURE[collection_name]["timestamp"]
            ].timestamp(),
            "props": {
                **{
                    key["name"]: document[key["name"]]
                    if key["name"] in document
                    else key["default"]
                    for key in MIXPANEL_IMPORT_STRUCTURE[collection_name]["props"]
                },
                **{
                    "$insert_id": document[
                        MIXPANEL_IMPORT_STRUCTURE[collection_name]["$insert_id"]
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
                    "from": f"Mongodb, Payments",
                    "data": json.dumps(obj, indent=4, default=str),
                    "time": datetime.now(),
                }
            )
        )
        set_last_entry("Payment", count + skip + 1)


def fetch_and_upload_transactions(skip=0):
    for count, transaction in enumerate(Transaction.objects.skip(skip)):
        collection_name = "transactions"
        document = transaction.to_mongo().to_dict()
        document["_id"] = str(document["_id"])
        obj = {
            "distinct_id": document[
                MIXPANEL_IMPORT_STRUCTURE[collection_name]["distinct_id"]
            ],
            "event_name": MIXPANEL_IMPORT_STRUCTURE[collection_name]["event_name"],
            "timestamp": document[
                MIXPANEL_IMPORT_STRUCTURE[collection_name]["timestamp"]
            ].timestamp(),
            "props": {
                **{
                    key["name"]: document[key["name"]]
                    if key["name"] in document
                    else key["default"]
                    for key in MIXPANEL_IMPORT_STRUCTURE[collection_name]["props"]
                },
                **{
                    "$insert_id": document[
                        MIXPANEL_IMPORT_STRUCTURE[collection_name]["$insert_id"]
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
                    "from": f"Mongodb, transactions",
                    "data": json.dumps(obj, indent=4, default=str),
                    "time": datetime.now(),
                }
            )
        )
        set_last_entry("Transaction", count + skip + 1)


def start():
    try:
        connect(
            db=config["MONGODB"]["db"],
            host=config["MONGODB"]["uri"],
        )

        # Upload payment events
        print("Payment Event Upload Started")
        payment_last_entry = get_last_entry()["Payment"]
        if payment_last_entry == 0:
            fetch_and_upload_payments()
        else:
            s = input(
                f"{payment_last_entry} Payment events uploaded, do want to start from zero again (Y/n) : "
            )
            if s in ["Y", "y"]:
                fetch_and_upload_payments()
            else:
                fetch_and_upload_payments(payment_last_entry)

        # Upload transaction events
        print("Transaction Event Upload Started")
        transaction_last_entry = get_last_entry()["Transaction"]
        if transaction_last_entry == 0:
            fetch_and_upload_transactions()
        else:
            s = input(
                f"{transaction_last_entry} Payment events uploaded, do want to start from zero again (Y/n) : "
            )
            if s in ["Y", "y"]:
                fetch_and_upload_transactions()
            else:
                fetch_and_upload_transactions(transaction_last_entry)
    except Exception as e:
        print(e)
        logging.error(str(e))

    finally:
        disconnect()
