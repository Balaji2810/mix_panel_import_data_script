import mongoengine as db
from mongoengine import Document

from datetime import datetime
from enum import Enum


class PaymentStatus(str, Enum):
    PENDING = "pending"
    CANCELLED = "cancelled"
    CAPTURED = "captured"
    CREDITED = "credited"


class ProductEnum(str, Enum):
    # spent
    PaidPlans = "paid-plans"
    PaidPlansUpgrade = "paid-plans-upgrade"
    PlanBacktest = "plan-backtest"
    PaidBacktest = "paid-backtests"
    CreditsExpiration = "credits-expiration"
    # topup
    CreditsPurchase = "credits-purchase"
    ReferralProgram = "referral-program"


class Payment(Document):
    meta = {"collection": "payments"}
    user = db.StringField(required=True)
    timestamp = db.DateTimeField(required=True, default=lambda: datetime.utcnow())
    razorpay_order = db.StringField(required=True)
    razorpay_payment = db.StringField(null=True, default=True)
    amount = db.IntField(required=True)
    credits = db.IntField(required=True, default=0)
    product_id = db.StringField(required=True, default="")
    product_name = db.StringField(required=True, default="")
    transaction = db.StringField(null=True, default=True)
    status = db.EnumField(PaymentStatus, required=True)


class Transaction(Document):
    meta = {"collection": "transactions"}
    user = db.StringField(required=True)
    type = db.StringField(required=True, choices=["topup", "spent"])
    timestamp = db.DateTimeField(required=True, default=lambda: datetime.utcnow())
    amount = db.IntField(required=True)
    product = db.EnumField(ProductEnum, required=True)
    reference = db.StringField(required=True, default="")
