import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer  

# Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "logistics_events"

def generate_to_kafka():
    companies = [f"LOGI_CORP_{i:02d}" for i in range(1, 11)]
    drivers = [f"DRV_{i:03d}" for i in range(1, 73)]
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", 
        "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
        "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", 
        "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore"
    ]

    notes = [
        "Routine check-in.", "Heavy traffic.", "Engine temp rising.", 
        "Road blocked by flood.", "Customs delay.", "Fueling up.",
        "Accident ahead.", "Tire pressure low.", "On schedule.", "Headlights damaged", "Everything is ok", "No Issues", "Fine", "OK", "No Issues found", "NA", "Nothing to worry about", "will reach on time", "No Problem"
    ]
    
    while True:
        source, dest = random.sample(cities, 2)
        
        # Base data
        data = {
            "company_id": random.choice(companies),
            "truck_id": f"TRK-{random.randint(1000, 9999)}",
            "driver_id": random.choice(drivers),
            "source_city": source,
            "dest_city": dest,
            "engine_temp": round(random.uniform(180.0, 230.0), 1),
            "average_speed": round(random.uniform(30.0, 80.0), 1),
            "fuel_remaining_percent": round(random.uniform(0.02, 1.0), 2),
            "weather_condition": random.choice(["Clear", "Rain", "Snow", "Tornado", "hailstorm", "Thunderstorm", "High Winds"]),
            "cargo_weight_kg": random.randint(5000, 40000),
            "timestamp": datetime.now().isoformat(),
	    "driver_note": random.choice(notes)
        }
        
        # SEND to Kafka
        producer.send(TOPIC_NAME, value=data)
        print(f"Sent to Kafka: {data['truck_id']} -> {dest}")
        
        time.sleep(5)

# Run the producer
if __name__ == "__main__":
    generate_to_kafka()