# consumers/payment_service.py

import sys
sys.path.append('..')
from utils.kafka_client import get_consumer
import time
import random

consumer = get_consumer('order-placed', 'payment-service')

print("💳 Payment Service listening...\n")

for message in consumer:
    order = message.value
    
    try:
        print(f"[Payment] Order #{order['order_id']} received")
        print(f"[Payment] Charging ₹{order['amount']} from {order['user_id']}")
        
        time.sleep(0.5)   # simulate payment gateway call
        
        # simulate random payment failure (20% chance)
        if random.random() < 0.2:
            raise Exception("Payment gateway timeout")
        
        print(f"[Payment] ✅ Payment of ₹{order['amount']} successful\n")
        consumer.commit()

    except Exception as e:
        print(f"[Payment] ❌ Payment failed: {e}")
        # don't commit → will retry
        # but what if it keeps failing? → need DLQ (coming next)
