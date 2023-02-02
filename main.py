from mixpanel import Mixpanel
import configparser
import logging
import mongodb_import
import mongodb_dump_import

logging.basicConfig(filename="log.txt", level=logging.DEBUG)
config = configparser.ConfigParser()
config.read("config.ini")

project_token = config["MIXPANEL"]["project_token"]
api_key = config["MIXPANEL"]["api_key"]


mp = Mixpanel(project_token)


def import_data(distinct_id, event_name, timestamp, props):
    mp.import_data(
        api_key, distinct_id, event_name, timestamp, props, api_secret=api_key
    )


if __name__ == "__main__":
    # Import data from MongoDB to MixPanel
    # mongodb_import.start()

    # Import data from dump(gzip) file
    mongodb_dump_import.start()
