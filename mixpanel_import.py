from mixpanel import Mixpanel
import configparser
import logging
from config import MIXPANEL_IMPORT_STRUCTURE, LOG_FORMAT
import json
from datetime import datetime


config = configparser.ConfigParser()
config.read("config.ini")

project_token = config["MIXPANEL"]["project_token"]
api_key = config["MIXPANEL"]["api_key"]


mp = Mixpanel(project_token)


def import_data(document, collection_name, log_title):
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

    mp.import_data(
        api_key,
        obj["distinct_id"],
        obj["event_name"],
        obj["timestamp"],
        obj["props"],
        api_secret=api_key,
    )
    logging.info(
        LOG_FORMAT.format(
            **{
                "from": log_title,
                "data": json.dumps(obj, indent=4, default=str),
                "time": datetime.now(),
            }
        )
    )
