import random
import uuid
from decimal import Decimal
from typing import Optional

MERCHANT_IDS = [
    "merchant_flipkart",
    "merchant_amazon",
    "merchant_swiggy",
    "merchant_zomato",
    "merchant_ola",
    "merchant_uber",
    "merchant_myntra",
    "merchant_nykaa",
]

PAYMENT_METHODS = ["UPI", "CARD", "NETBANKING", "WALLET"]

CURRENCIES = ["INR"]

AMOUNT_RANGES = {
    "merchant_flipkart": (199, 49999),
    "merchant_amazon": (99, 99999),
    "merchant_swiggy": (49, 1999),
    "merchant_zomato": (49, 1999),
    "merchant_ola": (30, 999),
    "merchant_uber": (30, 999),
    "merchant_myntra": (299, 9999),
    "merchant_nykaa": (199, 4999),
}


def generate_txn_id(prefix: str = "sim") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def generate_payment(
    merchant_id: Optional[str] = None,
    force_duplicate_of: Optional[str] = None,
    force_timeout: bool = False,
) -> dict:
    """
    Generate a single realistic payment payload.

    Args:
        merchant_id:        Pin to a specific merchant. Random if None.
        force_duplicate_of: Use this txn_id to simulate a duplicate submission.
        force_timeout:      Use merchant_timeout_test to guarantee a BANK_TIMEOUT.
    """
    if force_timeout:
        merchant = "merchant_timeout_test"
    else:
        merchant = merchant_id or random.choice(MERCHANT_IDS)

    min_amount, max_amount = AMOUNT_RANGES.get(merchant, (100, 9999))
    amount = Decimal(str(random.randint(min_amount * 100, max_amount * 100) / 100))

    txn_id = force_duplicate_of or generate_txn_id()

    return {
        "txn_id": txn_id,
        "merchant_id": merchant,
        "amount": str(amount),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "metadata": {
            "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
            "session_id": f"SES_{uuid.uuid4().hex[:8].upper()}",
        },
    }


def generate_batch(
    count: int,
    duplicate_rate: float = 0.05,
    timeout_rate: float = 0.03,
) -> list[dict]:
    """
    Generate a batch of payment payloads with controlled failure scenarios.

    duplicate_rate: fraction of requests that are duplicates of earlier ones
    timeout_rate:   fraction of requests that always result in BANK_TIMEOUT
    """
    payloads: list[dict] = []
    submitted_ids: list[str] = []

    for _ in range(count):
        roll = random.random()

        if submitted_ids and roll < duplicate_rate:
            original_id = random.choice(submitted_ids)
            payload = generate_payment(force_duplicate_of=original_id)

        elif roll < duplicate_rate + timeout_rate:
            payload = generate_payment(force_timeout=True)
            payload["txn_id"] = generate_txn_id("sim_timeout")
            submitted_ids.append(payload["txn_id"])

        else:
            payload = generate_payment()
            submitted_ids.append(payload["txn_id"])

        payloads.append(payload)

    return payloads
