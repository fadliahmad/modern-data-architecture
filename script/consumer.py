from kafka import KafkaConsumer
import requests
import psycopg2
import json

# Setup PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres",
    port="5433"
)
cur = conn.cursor()

# Kafka Consumer setup
consumer = KafkaConsumer(
    'loan_prediction_topic',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Mapping from label to index (for prediction)
home_ownership_map = {
    'RENT': 0,
    'MORTGAGE': 1,
    'OWN': 2,
    'OTHER': 3
}

loan_intent_map = {
    'EDUCATION': 0,
    'MEDICAL': 1,
    'VENTURE': 2,
    'PERSONAL': 3,
    'DEBTCONSOLIDATION': 4,
    'HOMEIMPROVEMENT': 5
}

loan_grade_map = {
    'A': 0,
    'B': 1,
    'C': 2,
    'D': 3,
    'E': 4,
    'F': 5
}

default_on_file_map = {
    'N': 0,
    'Y': 1
}

# Start consuming from Kafka
for msg in consumer:
    original_data = msg.value
    print("Received data:", original_data)

    try:
        # Buat salinan data untuk kebutuhan prediction (dengan encoded/indexed)
        data_for_prediction = {
            "person_age": original_data["person_age"],
            "person_income": original_data["person_income"],
            "person_home_ownership_index": home_ownership_map[original_data["person_home_ownership"]],
            "person_emp_length": original_data["person_emp_length"],
            "loan_intent_index": loan_intent_map[original_data["loan_intent"]],
            "loan_grade_index": loan_grade_map[original_data["loan_grade"]],
            "loan_amnt": original_data["loan_amnt"],
            "loan_int_rate": original_data["loan_int_rate"],
            "loan_percent_income": original_data["loan_percent_income"],
            "cb_person_default_on_file_index": default_on_file_map[original_data["cb_person_default_on_file"]],
            "cb_person_cred_hist_length": original_data["cb_person_cred_hist_length"]
        }

        # Request prediction ke API
        response = requests.post("http://localhost:8000/predict", json=data_for_prediction)
        prediction = response.json().get("prediction", None)
        print("Prediction result:", prediction)

        # Insert ke PostgreSQL (pakai data asli ditambah prediction)
        insert_query = """
            INSERT INTO loan_predictions (
                person_age, person_income, person_home_ownership, person_emp_length,
                loan_intent, loan_grade, loan_amnt, loan_int_rate, loan_percent_income,
                cb_person_default_on_file, cb_person_cred_hist_length, predictions
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            original_data["person_age"],
            original_data["person_income"],
            original_data["person_home_ownership"],
            original_data["person_emp_length"],
            original_data["loan_intent"],
            original_data["loan_grade"],
            original_data["loan_amnt"],
            original_data["loan_int_rate"],
            original_data["loan_percent_income"],
            original_data["cb_person_default_on_file"],
            original_data["cb_person_cred_hist_length"],
            prediction
        ))
        conn.commit()

    except Exception as e:
        print("Error:", e)