import json
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_events(num_events=1000):
    events = []
    user_ids = [str(uuid.uuid4())[:8] for _ in range(50)]
    event_types = ['post', 'like', 'share', 'comment', 'follow']
    
    for _ in range(num_events):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(user_ids),
            "event_type": random.choice(event_types),
            "content_id": str(uuid.uuid4())[:8],
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
            "metadata": {
                "device": random.choice(["iOS", "Android", "Web"]),
                "region": fake.country_code()
            }
        }
        events.append(event)
    
    os.makedirs('data/raw', exist_ok=True)
    file_path = f"data/raw/events_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    with open(file_path, 'w') as f:
        json.dump(events, f)
    print(f"Generated {num_events} events at {file_path}")

if __name__ == "__main__":
    generate_events(2000)