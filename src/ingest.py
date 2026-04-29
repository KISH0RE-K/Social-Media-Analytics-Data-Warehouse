import pandas as pd
import glob
import os
import json

def validate_and_stage():
    raw_path = 'data/raw/*.json'
    files = glob.glob(raw_path)
    
    all_data = []
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            all_data.extend(data)
            
    df = pd.DataFrame(all_data)
    
    if df['event_id'].isnull().any():
        print("Warning: Null event IDs found. Cleaning...")
        df = df.dropna(subset=['event_id'])
        
    # Standardize timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    os.makedirs('data/staging', exist_ok=True)
    df.to_parquet('data/staging/staged_events.parquet', index=False)
    print("Ingestion complete. Data staged as Parquet.")

if __name__ == "__main__":
    validate_and_stage()