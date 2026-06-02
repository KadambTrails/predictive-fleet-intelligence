import os
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib

load_dotenv()

client = clickhouse_connect.get_client(
    host=os.getenv("CH_HOST"),
    port=int(os.getenv('CH_PORT')),
    username=os.getenv('CH_USER'),
    password=os.getenv('CH_PASSWORD')
)


df = client.query_df('SELECT * FROM truck_logistics_gold.gold_truck_ml_features')

features = [
    'engine_temp',
    'avg_speed_mph',
    'fuel_percent',
    'cargo_weight_kg'
]

X = df[features]
y = df['label_maintenance_required']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("training model")

rf_model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
rf_model.fit(X_train, y_train)

y_pred = rf_model.predict(X_test)
print(f"\nModel Accuracy: {accuracy_score(y_test, y_pred) * 100:.2f}%")
print("\nDetailed Report:")
print(classification_report(y_test, y_pred))

os.makedirs('models_registry', exist_ok=True)
joblib.dump(rf_model, 'models_registry/maintenance_rf_v1.pkl')
print("\n Model saved to models_registry/maintenance_rf_v1.pkl")
