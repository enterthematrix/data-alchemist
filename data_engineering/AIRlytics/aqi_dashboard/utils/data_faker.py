import os
import gzip
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

MAX_COMPRESSED_SIZE_MB = 10
OUTPUT_DIR = Path("synthetic_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Load the reference data
with gzip.open("../../aqi_dag/data/2025_07_03/air_quality_data_2025_07_03_17_00_03.json.gz", "rt", encoding="utf-8") as f:
    real_data = json.load(f)

template_records = real_data["records"]

# Organize by station and pollutant
template_by_key = defaultdict(list)
for rec in template_records:
    key = (rec["state"], rec["city"], rec["station"], rec["pollutant_id"])
    template_by_key[key].append(rec)

# Synthetic generator
def generate_synthetic_value(val: float) -> float:
    noise = random.uniform(-0.15, 0.15)
    return max(0, round(val * (1 + noise), 1))

def generate_day_records(date: datetime) -> list:
    records = []
    for hour in range(24):
        ts = (date + timedelta(hours=hour)).strftime("%d-%m-%Y %H:00:00")
        for key, templates in template_by_key.items():
            state, city, station, pollutant = key
            base = templates[0]
            try:
                avg = float(base["avg_value"])
                min_v = float(base["min_value"])
                max_v = float(base["max_value"])
            except ValueError:
                continue

            record = {
                "country": "India",
                "state": state,
                "city": city,
                "station": station,
                "last_update": ts,
                "latitude": base["latitude"],
                "longitude": base["longitude"],
                "pollutant_id": pollutant,
                "min_value": str(generate_synthetic_value(min_v)),
                "max_value": str(generate_synthetic_value(max_v)),
                "avg_value": str(generate_synthetic_value(avg)),
            }
            records.append(record)
    return records

# Accumulate records until we near 250MB compressed
def save_chunk(chunk_index, records):
    out_data = {
        "index_name": real_data["index_name"],
        "title": real_data["title"],
        "desc": real_data["desc"],
        "org_type": real_data["org_type"],
        "org": real_data["org"],
        "records": records,
    }
    filename = f"air_quality_data_part_{chunk_index:03}.json.gz"
    file_path = OUTPUT_DIR / filename
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(out_data, f)

    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"Saved {file_path} ({size_mb:.2f} MB)")
    return size_mb

# Main loop
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 7, 2)
current_date = start_date
chunk_index = 1
buffer_records = []

test_path = OUTPUT_DIR / "temp_test.json.gz"

print(f"Starting data generation from {start_date.date()} to {end_date.date()}")
print("Generating and aggregating data...\n")

while current_date <= end_date:
    print(f"⏳ Processing {current_date.strftime('%Y-%m-%d')}...")

    # Generate today's records once
    todays_records = generate_day_records(current_date)
    print(f"📈 Generated {len(todays_records)} records.")

    # Combine with current buffer
    candidate_records = buffer_records + todays_records

    # Estimate size of combined record set
    test_data = {
        "index_name": real_data["index_name"],
        "title": real_data["title"],
        "desc": real_data["desc"],
        "org_type": real_data["org_type"],
        "org": real_data["org"],
        "records": candidate_records,
    }

    # Write once to temp file
    with gzip.open(test_path, "wt", encoding="utf-8") as f:
        json.dump(test_data, f)

    size_mb = os.path.getsize(test_path) / (1024 * 1024)
    print(f"💾 Estimated compressed size: {size_mb:.2f} MB")

    if size_mb < MAX_COMPRESSED_SIZE_MB:
        buffer_records = candidate_records
        print("📦 Added to current chunk.\n")
    else:
        # Save current buffer (without today's data)
        print(f"🚨 Size limit exceeded. Saving chunk {chunk_index}...\n")
        save_chunk(chunk_index, buffer_records)
        chunk_index += 1

        # Start new buffer with today's records
        buffer_records = todays_records

    current_date += timedelta(days=1)

# Save any remaining buffer
if buffer_records:
    print(f"✅ Saving final chunk {chunk_index} with {len(buffer_records)} records.")
    save_chunk(chunk_index, buffer_records)

# Delete temp test file
if test_path.exists():
    os.remove(test_path)

print("\n🎉 All chunks generated successfully.")
