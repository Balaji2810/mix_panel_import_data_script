from mongoengine import connect, disconnect
import configparser

from models import Payment, Transaction, ProductEnum, PaymentStatus
from uuid import uuid4
import random
from datetime import timedelta, datetime


config = configparser.ConfigParser()
config.read("config.ini")


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


user_ids = [
    "1111_dummy",
    "2222_dummy",
    "3333_dummy",
    "4444_dummy",
    "5555_dummy",
    "6666_dummy",
    "7777_dummy",
    "8888_dummy",
]
amounts = [499, 999, 899, 799, 699, 599, 199]
t_types = ["topup", "spent"]


def load_data(count=100):
    try:
        connect(
            db=config["MONGODB"]["db"],
            host=config["MONGODB"]["uri"],
        )
        for i in range(count):
            print("Uploading Data :", i + 1)
            t_id = uuid4().hex
            u_id = random.choice(user_ids)
            time_stamp = random_date(datetime(2022, 7, 1), datetime(2023, 1, 25))
            amount = random.choice(amounts)
            t_type = random.choice(t_types)
            product = random.choice(list(ProductEnum))
            t_type = random.choice(t_types)
            product = random.choice(list(ProductEnum))
            if t_type == t_types[1]:
                while product in [
                    ProductEnum.CreditsPurchase,
                    ProductEnum.ReferralProgram,
                ]:
                    product = random.choice(list(ProductEnum))
            else:
                while product not in [
                    ProductEnum.CreditsPurchase,
                    ProductEnum.ReferralProgram,
                ]:
                    product = random.choice(list(ProductEnum))
            if product == ProductEnum.CreditsPurchase:
                p_status = random.choice(list(PaymentStatus))
                payment = Payment(
                    user=u_id,
                    timestamp=time_stamp,
                    razorpay_order=uuid4().hex,
                    amount=amount,
                    credits=amount if p_status == PaymentStatus.CREDITED else 0,
                    product_id=product.name,
                    product_name=product.value,
                    transaction=t_id,
                    status=p_status,
                )
                payment.save()

            transaction = Transaction(
                user=u_id,
                type=t_type,
                timestamp=time_stamp,
                amount=amount,
                product=product,
                reference=t_id,
            )
            transaction.save()
    except Exception as e:
        print(e)
    finally:
        disconnect()


if __name__ == "__main__":
    count = 1
    print("Trying to load Dummy Data")
    # Do not run this function in prod, this is only for testing
    # load_data(count)
    print(f"Loaded {count} Dummy Data")
