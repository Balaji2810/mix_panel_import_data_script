import logging
import mongodb_dump_import
import mongodb_import

logging.basicConfig(filename="log.txt", level=logging.DEBUG)


if __name__ == "__main__":
    # Import data from MongoDB to MixPanel
    # mongodb_import.start()

    # Import data from dump(gzip) file
    mongodb_dump_import.start()
