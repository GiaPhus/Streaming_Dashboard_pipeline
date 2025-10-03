import requests
import json
import random
import uuid
from datetime import datetime

PRODUCTS = [
    {"name": "Laptop", "price": 1000},
    {"name": "Headphones", "price": 100},
    {"name": "Keyboard", "price": 50},
    {"name": "Mouse", "price": 30},
    {"name": "Monitor", "price": 300},
    {"name": "Phone", "price": 800},
    {"name": "Tablet", "price": 600},
]

def get_random_user():
    """Fetch random user from API"""
    url = "https://randomuser.me/api/"
    response = requests.get(url)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if not results:
            raise Exception("No user returned from API")
        return results[0]
    else:
        raise Exception(f"API error: {response.status_code}")


def generate_purchase(user_id):
    """Generate random purchase info linked to a user"""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    total = product["price"] * quantity
    return {
        "purchase_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product": product["name"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total": total,
        "timestamp": datetime.utcnow().isoformat()
    }

def get_user_with_purchase():
    user = get_random_user()
    user_id = user["login"]["uuid"]

    purchase = generate_purchase(user_id)

    user_with_purchase = {
        "user": user,
        "purchase": purchase
    }
    return user_with_purchase


if __name__ == "__main__":
    record = get_user_with_purchase()
    print(json.dumps(record, indent=2, ensure_ascii=False))
