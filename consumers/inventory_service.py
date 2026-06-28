# consumers/inventory_service.py

import sys
sys.path.append('..')
from utils.kafka_client import get_consumer
import time

consumer = get_consumer('order-placed', 'inventory-service')

print("📦 Inventory Service listening...\n")

for message in consumer:
    order = message.value
    
    try:
        print(f"[Inventory] Order #{order['order_id']} received")
        print(f"[Inventory] Reducing stock for: {order['items']}")
        
        # simulate DB update
        time.sleep(0.3)
        
        for item in order['items']:
            print(f"[Inventory] ✅ Stock reduced for '{item}'")
        
        consumer.commit()
        print(f"[Inventory] Offset committed\n")

    except Exception as e:
        print(f"[Inventory] ❌ Failed: {e}")
        # don't commit → message will be reprocessed
