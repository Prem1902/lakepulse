import os
import json
import uuid
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession

class APIIngestionClient:
    """Fetches paginated customer risk records with exponential backoff and lands to UC Volume."""

    def __init__(self, base_url: str, api_token: str, catalog: str):
        self.base_url = base_url
        self.catalog = catalog
        self.session = self._build_resilient_session(api_token)
        self.volume_base_path = f"/Volumes/{self.catalog}/landing/raw_api_json"

    @staticmethod
    def _build_resilient_session(api_token: str) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        })
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def fetch_and_land(self, batch_id: str) -> int:
        page = 1
        total_records = 0
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target_dir = f"{self.volume_base_path}/payloads/dt={ingestion_date}"
        os.makedirs(target_dir, exist_ok=True)

        while True:
            if self.base_url == "SIMULATE":
                records = self._simulate_api_payload(page)
                has_next = False
            else:
                response = self.session.get(
                    f"{self.base_url}/v1/customers/updates",
                    params={"page": page, "limit": 500},
                    timeout=30
                )
                response.raise_for_status()
                payload = response.json()
                records = payload.get("data", [])
                has_next = payload.get("has_next", False)

            if not records:
                break

            file_path = f"{target_dir}/batch_{batch_id}_page_{page}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                for rec in records:
                    rec["_batch_id"] = batch_id
                    rec["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    f.write(json.dumps(rec) + "\n")

            total_records += len(records)
            if not has_next:
                break
            page += 1

        return total_records

    @staticmethod
    def _simulate_api_payload(page: int) -> list:
        now_iso = datetime.now(timezone.utc).isoformat()
        return [
            {
                "customer_id": "CUST-1001",
                "full_name": "Aarav Sharma",
                "email": "aarav.sharma@example.com",
                "credit_tier": "PLATINUM",
                "credit_limit": 250000.00,
                "risk_score": 790,
                "address_city": "Bengaluru",
                "updated_at": now_iso
            },
            {
                "customer_id": "CUST-1002",
                "full_name": "Meera Nair",
                "email": "meera.nair@example.com",
                "credit_tier": "SILVER",
                "credit_limit": 75000.00,
                "risk_score": 680,
                "address_city": "Kochi",
                "updated_at": now_iso
            }
        ]

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    catalog = spark.conf.get("bundle.catalog", "dev_fin_catalog")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        api_token = dbutils.secrets.get(scope="fintech_api_scope", key="bearer_token")
        api_url = spark.conf.get("bundle.api_url", "SIMULATE")
    except Exception:
        api_token, api_url = "dummy_token", "SIMULATE"

    client = APIIngestionClient(base_url=api_url, api_token=api_token, catalog=catalog)
    landed_count = client.fetch_and_land(batch_id=batch_id)

    dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
    dbutils.jobs.taskValues.set(key="api_landed_count", value=landed_count)
    print(f"[SUCCESS] Batch {batch_id}: Landed {landed_count} records to UC Volume.")
