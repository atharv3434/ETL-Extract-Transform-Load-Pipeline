import csv
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Configure logging telemetry
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)s │ %(message)s")
logger = logging.getLogger(__name__)

class EcommerceETL:
    def __init__(self, db_path: str = "analytics_warehouse.db"):
        self.db_path = db_path
        # Dynamic static currency conversion mappings to USD base
        self.exchange_rates = {"USD": 1.0, "EUR": 1.08, "INR": 0.012}

    # ── STAGE 1: EXTRACT ──────────────────────────────────────────────────────
    def extract_transactions(self, csv_path: str) -> List[Dict]:
        """Reads transactions from raw CSV files."""
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"Missing transaction source file: {csv_path}")
        
        transactions = []
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                transactions.append(dict(row))
        logger.info(f"Successfully extracted {len(transactions)} records from CSV.")
        return transactions

    def extract_users(self, json_path: str) -> Dict[str, Dict]:
        """Reads metadata from JSON files and keys it by user_id."""
        if not Path(json_path).exists():
            raise FileNotFoundError(f"Missing user metadata source file: {json_path}")
        
        with open(json_path, mode='r', encoding='utf-8') as f:
            users_list = json.load(f)
        
        # Optimize search overhead by restructuring into a map lookup table
        user_map = {user["user_id"]: user for user in users_list}
        logger.info(f"Successfully extracted {len(user_map)} user profiles from JSON.")
        return user_map

    # ── STAGE 2: TRANSFORM ────────────────────────────────────────────────────
    def transform(self, transactions: List[Dict], user_map: Dict[str, Dict]) -> List[Tuple]:
        """Cleans, normalizes, and enriches data metrics."""
        transformed_records = []
        skipped_count = 0

        for txn in transactions:
            try:
                # 1. Validation: Skip corrupt/empty values or anomalous negative pricing
                if not txn["amount"] or float(txn["amount"]) <= 0:
                    skipped_count += 1
                    continue
                
                raw_amount = float(txn["amount"])
                currency = txn["currency"]
                
                # 2. Currency Standardization: Convert everything to USD base values
                rate = self.exchange_rates.get(currency, 1.0)
                amount_usd = round(raw_amount * rate, 2)
                
                # 3. Data Enrichment: Map sign-up location and profile tiers
                uid = txn["user_id"]
                user_info = user_map.get(uid, {"signup_country": "UNKNOWN", "is_premium": False})
                
                # 4. Parsing string timestamps to standard SQL formatting strings
                parsed_time = datetime.strptime(txn["timestamp"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

                record = (
                    txn["transaction_id"],
                    uid,
                    txn["product_id"],
                    amount_usd,
                    user_info["signup_country"],
                    1 if user_info["is_premium"] else 0,
                    parsed_time
                )
                transformed_records.append(record)
            except Exception as err:
                logger.warning(f"Skipping record due to mapping distortion structural faults: {txn}. Error: {err}")
                skipped_count += 1

        logger.info(f"Transformation complete. {len(transformed_records)} records processed ({skipped_count} skipped).")
        return transformed_records

    # ── STAGE 3: LOAD ─────────────────────────────────────────────────────────
    def load(self, records: List[Tuple]):
        """Writes processed rows safely into an SQLite database warehouse table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # DDL Definition: Ensure target warehouse infrastructure table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT,
                product_id TEXT,
                amount_usd REAL,
                country TEXT,
                is_premium INTEGER,
                transaction_time TEXT
            )
        """)

        # Execute UPSERT transaction logic strategy array
        cursor.executemany("""
            INSERT OR REPLACE INTO fact_sales 
            (transaction_id, user_id, product_id, amount_usd, country, is_premium, transaction_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, records)

        conn.commit()
        
        # Verify execution record calculations
        cursor.execute("SELECT COUNT(*) FROM fact_sales")
        total_rows = cursor.fetchone()[0]
        conn.close()
        
        logger.info(f"Load stage complete. Database current state: {total_rows} active transactional logs indexed.")

    # Run Sequence Controller
    def run_pipeline(self, csv_path: str, json_path: str):
        logger.info("Starting system ETL Pipeline sequence...")
        raw_txns = self.extract_transactions(csv_path)
        raw_users = self.extract_users(json_path)
        clean_records = self.transform(raw_txns, raw_users)
        self.load(clean_records)
        logger.info("ETL Pipeline pipeline sequence terminated successfully.")

if __name__ == "__main__":
    # Local run execution trigger
    pipeline = EcommerceETL()
    pipeline.run_pipeline("data/raw_transactions.csv", "data/user_metadata.json")