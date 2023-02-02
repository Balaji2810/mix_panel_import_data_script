import configparser
from models import Payment, Transaction
from mongoengine import connect, disconnect
import json
import logging
import main

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
    for count, payment in enumerate(
        Payment.objects.only("user", "timestamp", "amount", "credits").skip(skip)
    ):
        props = {
            "amount": payment.amount,
            "credits": payment.credits,
        }

        main.import_data(payment.user, "Payment", payment.timestamp, props)
        print(count + skip + 1, props, payment.timestamp)

        logging.info(
            "Entry to MixPanel : Payments\n"
            + "payment_id : "
            + str(payment.id)
            + "\nuser_id : "
            + payment.user
            + "\n"
            + str(count + skip + 1)
            + " "
            + str(props)
            + " "
            + str(payment.timestamp)
            + "\n"
        )
        set_last_entry("Payment", count + skip + 1)


def fetch_and_upload_transactions(skip=0):
    for count, transaction in enumerate(
        Transaction.objects.only("user", "timestamp", "amount", "product", "type").skip(
            skip
        )
    ):
        props = {
            "product": transaction.product,
            "type": transaction.type,
            "amount": transaction.amount,
        }

        main.import_data(transaction.user, "Transaction", transaction.timestamp, props)
        print(count + skip + 1, props, transaction.timestamp)
        logging.info(
            "Entry to MixPanel : Transactions\n"
            + "transaction_id : "
            + str(transaction.id)
            + "\nuser_id : "
            + transaction.user
            + "\n"
            + str(count + skip + 1)
            + " "
            + str(props)
            + " "
            + str(transaction.timestamp)
            + "\n"
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
