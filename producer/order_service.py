# producer/order_service.py

import sys
sys.path.append('..')
from utils.kafka_client import get_producer
import uuid
import time

producer = get_producer()

def place_order(user_id, items, total_amount):
    """
    When user places order → publish event to Kafka
    Order service does NOT call inventory/payment/email directly
    It just fires the event and forgets
    """

    order = {
        "order_id":   str(uuid.uuid4())[:8],   # random order id
        "user_id":    user_id,
        "items":      items,
        "amount":     total_amount,
        "status":     "placed",
        "timestamp":  time.time(),
        "message":    "this event is triggered"
    }

    # publish to 'order-placed' topic
    producer.send(
        'order-placed',
        key=user_id,
        value=order
    )
    producer.flush()

    print(f"\n🛒 Order placed!")
    print(f"   Order ID : {order['order_id']}")
    print(f"   User     : {user_id}")
    print(f"   Items    : {items}")
    print(f"   Amount   : ₹{total_amount}")
    print(f"   ✅ Event fired to Kafka → 'order-placed' topic\n")
    
    return order


# place 3 test orders
place_order("user_001", ["shoes", "tshirt"], 1500)
time.sleep(1)
place_order("user_002", ["laptop"],          45000)
time.sleep(1)
place_order("user_003", ["book", "pen"],     350)
