import os
import json
import hashlib
import urllib.request
from datetime import datetime, timezone
import pandas as pd
import numpy as np

DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
RAW_FILE = "data/yellow_tripdata_2024-01.parquet"
CLEAN_FILE = "data/cleaned_events.parquet"
SAMPLE_FILE = "data/sample_1000_events.csv"
MANIFEST_FILE = "data/data_manifest.json"
SEED = 42
TARGET_RECORDS = 25000

def compute_sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    os.makedirs("data", exist_ok=True)
    
    if not os.path.exists(RAW_FILE):
        print(f"Downloading {DATA_URL}...")
        urllib.request.urlretrieve(DATA_URL, RAW_FILE)
    
    download_time = datetime.now(timezone.utc).isoformat()
    file_sha256 = compute_sha256(RAW_FILE)
    
    df_raw = pd.read_parquet(RAW_FILE, columns=[
        "tpep_pickup_datetime", "PULocationID", "total_amount", "trip_distance"
    ])
    total_input_rows = len(df_raw)
    
    valid_mask = (
        df_raw["tpep_pickup_datetime"].notna() &
        df_raw["PULocationID"].notna() &
        df_raw["total_amount"].notna() &
        (df_raw["total_amount"] > 0) &
        (df_raw["total_amount"] <= 1000.0) &
        (df_raw["trip_distance"] > 0) &
        (df_raw["PULocationID"].between(1, 263))
    )
    
    df_valid = df_raw[valid_mask].copy()
    valid_rows_count = len(df_valid)
    rejected_rows_count = total_input_rows - valid_rows_count
    
    df_sampled = df_valid.sample(n=TARGET_RECORDS, random_state=SEED).sort_values("tpep_pickup_datetime").reset_index(drop=True)
    
    df_sampled["seq"] = np.arange(1, len(df_sampled) + 1, dtype=np.int64)
    df_sampled["event_time"] = pd.to_datetime(df_sampled["tpep_pickup_datetime"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df_sampled["category"] = "ZONE_" + (df_sampled["PULocationID"] % 10).astype(str)
    df_sampled["amount_cents"] = np.round(df_sampled["total_amount"] * 100).astype(np.int64)
    
    df_sampled["event_id"] = [
        hashlib.sha256(f"{et}_{cat}_{amt}_{seq}".encode()).hexdigest()[:24]
        for et, cat, amt, seq in zip(
            df_sampled["event_time"], df_sampled["category"], df_sampled["amount_cents"], df_sampled["seq"]
        )
    ]
    
    final_cols = ["event_id", "seq", "event_time", "category", "amount_cents"]
    df_out = df_sampled[final_cols]
    
    df_out.to_parquet(CLEAN_FILE, index=False)
    df_out.head(1000).to_csv(SAMPLE_FILE, index=False)
    
    manifest = {
        "source_url": DATA_URL,
        "download_timestamp_utc": download_time,
        "raw_file_name": RAW_FILE,
        "raw_file_sha256": file_sha256,
        "total_input_rows": int(total_input_rows),
        "valid_rows_after_cleaning": int(valid_rows_count),
        "rejected_rows": int(rejected_rows_count),
        "sampled_rows_for_replay": int(len(df_out)),
        "random_seed": SEED,
        "cleaning_rules": [
            "Drop rows with NULL in tpep_pickup_datetime, PULocationID, or total_amount",
            "Filter 0 < total_amount <= 1000.0 USD and trip_distance > 0",
            "Filter PULocationID in valid NYC taxi zone range [1, 263]",
            "Convert total_amount to integer amount_cents = round(total_amount * 100)",
            "Map PULocationID to 10 categories: ZONE_0 through ZONE_9"
        ]
    }
    
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        
    print("Data preparation complete! Manifest details:")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
