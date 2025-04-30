from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_random_data():
    return {
        "person_age": random.randint(18, 70),
        "person_income": random.randint(5000, 200000),
        "person_home_ownership": random.choice(['RENT', 'OWN', 'MORTGAGE', 'OTHER']),
        "person_emp_length": random.randint(0, 40),
        "loan_intent": random.choice(['PERSONAL', 'EDUCATION', 'DEBTCONSOLIDATION', 'MEDICAL', 'VENTURE', 'HOMEIMPROVEMENT']),
        "loan_grade": random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G']),
        "loan_amnt": random.randint(1000, 50000),
        "loan_int_rate": round(random.uniform(5.0, 30.0), 2),
        "loan_percent_income": round(random.uniform(0.05, 0.5), 2),
        "cb_person_default_on_file": random.choice(['Y', 'N']),
        "cb_person_cred_hist_length": random.randint(1, 30)
    }

while True:
    data_sample = generate_random_data()
    producer.send('loan_prediction_topic', value=data_sample)
    print("Sent data to Kafka:", data_sample)
    time.sleep(5)  # kirim data tiap 5 detik