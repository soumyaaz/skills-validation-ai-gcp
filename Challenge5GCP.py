PROJECT_ID = "qwiklabs-gcp-02-43b1f06a9ba5"
client = storage.Client(project=PROJECT_ID)

buckets = list(client.list_buckets(project=PROJECT_ID))
print("Buckets in project:")
for b in buckets:
print(" -", b.name)
BUCKET = "<qwiklabs-gcp-02-43b1f06a9ba5-airport-poc-37eaee2c>"
os.environ["BUCKET_NAME"] = BUCKET
DST_URI = f"gs://{BUCKET}/data/airports.csv"
print("DST_URI =", DST_URI)

from google.cloud import storage

storage_client = storage.Client(project=PROJECT_ID)

src_bucket = storage_client.bucket("labs.roitraining.com")
src_blob = src_bucket.blob("data-to-ai-workshop/airports.csv")

BUCKET = new_bucket_name

dst_bucket = storage_client.bucket(BUCKET)
src_bucket.copy_blob(src_blob, dst_bucket, new_name="data/airports.csv")

print("Copied to:", f"gs://{BUCKET}/data/airports.csv")

from google.cloud import bigquery
bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

ds_id = f"{PROJECT_ID}.{DATASET}"
bq.create_dataset(bigquery.Dataset(ds_id), exists_ok=True)

job = bq.load_table_from_uri(
f"gs://{BUCKET}/data/airports.csv",
f"{ds_id}.{AIRPORTS_RAW}",
job_config=bigquery.LoadJobConfig(
source_format=bigquery.SourceFormat.CSV,
skip_leading_rows=1,
autodetect=True,
write_disposition="WRITE_TRUNCATE",
),
)
job.result()

t = bq.get_table(f"{ds_id}.{AIRPORTS_RAW}")
print("Loaded rows:", t.num_rows)
print("Columns:", [f.name for f in t.schema])

airports_df = bq.query(f"SELECT * FROM {ds_id}.{AIRPORTS_RAW} LIMIT 5").to_dataframe()
airports_df.head()
from google.cloud import bigquery
bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

ds_id = f"{PROJECT_ID}.{DATASET}"
bq.create_dataset(bigquery.Dataset(ds_id), exists_ok=True)

job = bq.load_table_from_uri(
f"gs://{BUCKET}/data/airports.csv",
f"{ds_id}.{AIRPORTS_RAW}",
job_config=bigquery.LoadJobConfig(
source_format=bigquery.SourceFormat.CSV,
skip_leading_rows=1,
autodetect=True,
write_disposition="WRITE_TRUNCATE",
),
)
job.result()

t = bq.get_table(f"{ds_id}.{AIRPORTS_RAW}")
print("Loaded rows:", t.num_rows)
print("Columns:", [f.name for f in t.schema])

airports_df = bq.query(f"SELECT * FROM {ds_id}.{AIRPORTS_RAW} LIMIT 5").to_dataframe()
airports_df.head()



import json
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

import vertexai
from vertexai.generative_models import (
    GenerativeModel,
    GenerationConfig,
    SafetySetting,
    HarmCategory,
    HarmBlockThreshold,
)


PROJECT_ID = "qwiklabs-gcp-02-43b1f06a9ba5"
BQ_LOCATION = "US"              
VERTEX_LOCATION = "us-central1" 
DATASET = "airport_poc"
AIRPORTS_RAW = "airports_raw"
ALERTS_TABLE = "airport_alerts"

# --- Clients ---
bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
vertexai.init(project=PROJECT_ID, location=VERTEX_LOCATION)

# Gemini model available in your project/region
model = GenerativeModel("gemini-2.5-flash")



safety = [
    SafetySetting(HarmCategory.HARM_CATEGORY_HATE_SPEECH, HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE),
    SafetySetting(HarmCategory.HARM_CATEGORY_HARASSMENT, HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE),
    SafetySetting(HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE),
    SafetySetting(HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE),
]

ds_id = f"{PROJECT_ID}.{DATASET}"
airports_table_id = f"{ds_id}.{AIRPORTS_RAW}"
alerts_table_id = f"{ds_id}.{ALERTS_TABLE}"

#Destination Table
ddl = f"""
CREATE TABLE IF NOT EXISTS `{alerts_table_id}` (
  alert_id STRING,
  ident STRING,
  iata_code STRING,
  airport_name STRING,
  municipality STRING,
  iso_region STRING,
  latitude_deg FLOAT64,
  longitude_deg FLOAT64,
  severity STRING,
  category STRING,
  message STRING,
  recommended_action STRING,
  expires_hours INT64,
  model STRING,
  created_ts TIMESTAMP,
  prompt_version STRING,
  raw_response STRING
)
PARTITION BY DATE(created_ts)
"""
bq.query(ddl).result()

# --- Pull airports from BQ -- #
airports_df = bq.query(f"SELECT * FROM `{airports_table_id}`").to_dataframe()

PROMPT_VERSION = "v1"
created_ts = datetime.now(timezone.utc)

def build_prompt(r: pd.Series) -> str:
    return f"""
You generate a single generic airport operational alert for planning/training.
Do NOT claim real-world, real-time conditions (weather, incidents, outages). Keep it plausible and general.

Return ONLY valid JSON with exactly these keys:
severity (LOW|MEDIUM|HIGH),
category (Weather|Security|Maintenance|Staffing|ATC|Other),
message (string, <=240 chars),
recommended_action (string, <=240 chars),
expires_hours (integer 1-72)

Airport:
ident: {r.get('ident')}
iata_code: {r.get('iata_code')}
airport_name: {r.get('airport_name')}
municipality: {r.get('municipality')}
iso_region: {r.get('iso_region')}
lat: {r.get('latitude_deg')}
lon: {r.get('longitude_deg')}
""".strip()

rows = []
for idx, r in airports_df.iterrows():
    prompt = build_prompt(r)

    # Simple retry loop for transient failures / rate limits
    last_err = None
    for attempt in range(4):
        try:
            resp = model.generate_content(
                prompt,
                generation_config=gen_config,
                safety_settings=safety,
            )
            raw = resp.text.strip()

            obj = json.loads(raw)  # will raise if Gemini returns non-JSON
            alert_id = f"{r.get('ident','')}-{created_ts.strftime('%Y%m%dT%H%M%SZ')}"

            rows.append({
                "alert_id": alert_id,
                "ident": r.get("ident"),
                "iata_code": r.get("iata_code"),
                "airport_name": r.get("airport_name"),
                "municipality": r.get("municipality"),
                "iso_region": r.get("iso_region"),
                "latitude_deg": float(r.get("latitude_deg")) if r.get("latitude_deg") is not None else None,
                "longitude_deg": float(r.get("longitude_deg")) if r.get("longitude_deg") is not None else None,
                "severity": obj.get("severity"),
                "category": obj.get("category"),
                "message": obj.get("message"),
                "recommended_action": obj.get("recommended_action"),
                "expires_hours": int(obj.get("expires_hours")) if obj.get("expires_hours") is not None else None,
                "model": model._model_name,
                "created_ts": created_ts,
                "prompt_version": PROMPT_VERSION,
                "raw_response": raw,
            })
            break
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    else:
        rows.append({
            "alert_id": f"{r.get('ident','')}-{created_ts.strftime('%Y%m%dT%H%M%SZ')}",
            "ident": r.get("ident"),
            "iata_code": r.get("iata_code"),
            "airport_name": r.get("airport_name"),
            "municipality": r.get("municipality"),
            "iso_region": r.get("iso_region"),
            "latitude_deg": float(r.get("latitude_deg")) if r.get("latitude_deg") is not None else None,
            "longitude_deg": float(r.get("longitude_deg")) if r.get("longitude_deg") is not None else None,
            "severity": "LOW",
            "category": "Other",
            "message": "Alert generation failed; see raw_response.",
            "recommended_action": "Retry generation or inspect model output.",
            "expires_hours": 24,
            "model": model._model_name,
            "created_ts": created_ts,
            "prompt_version": PROMPT_VERSION,
            "raw_response": f"ERROR: {repr(last_err)}",
        })

alerts_df = pd.DataFrame(rows)

# --- Write to BigQuery ---
job = bq.load_table_from_dataframe(
    alerts_df,
    alerts_table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
)
job.result()

print(f"Wrote {len(alerts_df)} alerts to {alerts_table_id}")
