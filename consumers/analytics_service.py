# consumers/analytics_service.py

import sys
sys.path.append('..')
from utils.kafka_client import get_consumer

consumer = get_consumer('order-placed', 'analytics-service')
print("📊 Analytics Service listening...\n")

total_revenue = 0

for message in consumer:
    order = message.value
    try:
        total_revenue += order['amount']
        print(f"[Analytics] Order #{order['order_id']} logged")
        print(f"[Analytics] Total revenue so far: ₹{total_revenue}\n")
        consumer.commit()
    except Exception as e:
        print(f"[Analytics] ❌ Failed: {e}")
