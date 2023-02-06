from scripts.models import Payment, Transaction


MIXPANEL_IMPORT_STRUCTURE = {
    "payments": {
        "event_name": "Payments",
        "distinct_id": "user",
        "$insert_id": "_id",
        "timestamp": "timestamp",
        "props": [{"name": "amount", "default": 0}, {"name": "credits", "default": 0}],
        "collection": Payment,
    },
    "transactions": {
        "event_name": "Transactions",
        "distinct_id": "user",
        "$insert_id": "_id",
        "timestamp": "timestamp",
        "props": [
            {"name": "amount", "default": 0},
            {"name": "product", "default": ""},
            {"name": "type", "default": ""},
        ],
        "collection": Transaction,
    },
}

LOG_FORMAT = """
From : {from}
Exe Time : {time}
data :
{data}
"""
