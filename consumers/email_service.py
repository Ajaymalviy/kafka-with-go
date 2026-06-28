# consumers/email_service.py

import sys
sys.path.append('..')
from utils.kafka_client import get_consumer
import time

consumer = get_consumer('order-placed', 'email-service')
print("📧 Email Service listening...\n")

for message in consumer:
    order = message.value
    try:
        print(f"[Email] Sending confirmation to {order['user_id']}")
        time.sleep(0.2)
        print(f"[Email] ✅ Email sent for Order #{order['order_id']}\n")
        consumer.commit()
    except Exception as e:
        print(f"[Email] ❌ Failed: {e}")
