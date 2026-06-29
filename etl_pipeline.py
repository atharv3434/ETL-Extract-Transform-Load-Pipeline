import csv
import json
import sqlite3
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)s │ %(message)s")
logger = logging.getLogger(__name__)

class EcommerceETL:
    def __init__(self, db_path: str = "analytics_warehouse.db"):
        self.db_path = db_path
        self.exchange_rates = {"USD": 1.0, "EUR": 1.08, "INR": 0.012}

    # ── STAGE 1: EXTRACT ──────────────────────────────────────────────────────
    def extract_transactions(self, csv_path: str) -> List[Dict]:
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"Missing transaction source: {csv_path}")
        transactions = []
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                transactions.append(dict(row))
        logger.info(f"Extracted {len(transactions)} transactions from CSV.")
        return transactions

    def extract_users(self, json_path: str) -> Dict[str, Dict]:
        if not Path(json_path).exists():
            raise FileNotFoundError(f"Missing user metadata source: {json_path}")
        with open(json_path, mode='r', encoding='utf-8') as f:
            users_list = json.load(f)
        user_map = {user["user_id"]: user for user in users_list}
        logger.info(f"Extracted {len(user_map)} user profiles from JSON.")
        return user_map

    def extract_products(self, xml_path: str) -> Dict[str, Dict]:
        """Parses the XML product catalog dataset into an optimized map lookup."""
        if not Path(xml_path).exists():
            raise FileNotFoundError(f"Missing product catalog source: {xml_path}")
        
        product_map = {}
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        for prod in root.findall('product'):
            prod_id = prod.find('id').text
            category = prod.find('category').text
            supplier = prod.find('supplier').text
            
            product_map[prod_id] = {
                "category": category,
                "supplier": supplier
            }
            
        logger.info(f"Extracted {len(product_map)} product mappings from XML.")
        return product_map

    # ── STAGE 2: TRANSFORM ────────────────────────────────────────────────────
    def transform(self, transactions: List[Dict], user_map: Dict[str, Dict], product_map: Dict[str, Dict]) -> List[Tuple]:
        """Cleans, standardizes, and enriches transactions using User and Product maps."""
        transformed_records = []
        skipped_count = 0

        for txn in transactions:
            try:
                # Validation rules
                if not txn["amount"] or float(txn["amount"]) <= 0:
                    skipped_count += 1
                    continue
                
                raw_amount = float(txn["amount"])
                currency = txn["currency"]
                
                # Currency conversion strategy
                rate = self.exchange_rates.get(currency, 1.0)
                amount_usd = round(raw_amount * rate, 2)
                
                # Data Enrichment lookup steps
                uid = txn["user_id"]
                pid = txn["product_id"]
                
                user_info = user_map.get(uid, {"signup_country": "UNKNOWN", "is_premium": False})
                prod_info = product_map.get(pid, {"category": "UNCATEGORIZED", "supplier": "UNKNOWN"})
                
                parsed_time = datetime.strptime(txn["timestamp"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

                record = (
                    txn["transaction_id"],
                    uid,
                    pid,
                    amount_usd,
                    user_info["signup_country"],
                    1 if user_info["is_premium"] else 0,
                    prod_info["category"],
                    prod_info["supplier"],
                    parsed_time
                )
                transformed_records.append(record)
            except Exception as err:
                logger.warning(f"Skipping damaged operational row record {txn}. Error: {err}")
                skipped_count += 1

        logger.info(f"Transform phase complete. {len(transformed_records)} records ready.")
        return transformed_records

    # ── STAGE 3: LOAD ─────────────────────────────────────────────────────────
    def load(self, records: List[Tuple]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Updated DDL incorporating product metrics attributes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT,
                product_id TEXT,
                amount_usd REAL,
                country TEXT,
                is_premium INTEGER,
                product_category TEXT,
                product_supplier TEXT,
                transaction_time TEXT
            )
        """)

        cursor.executemany("""
            INSERT OR REPLACE INTO fact_sales 
            (transaction_id, user_id, product_id, amount_usd, country, is_premium, product_category, product_supplier, transaction_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM fact_sales")
        total_rows = cursor.fetchone()[0]
        conn.close()
        logger.info(f"Load complete. Data warehouse contains {total_rows} validated entries.")

    def run_pipeline(self, csv_path: str, json_path: str, xml_path: str):
        logger.info("Initializing multi-format ETL execution stream...")
        raw_txns = self.extract_transactions(csv_path)
        raw_users = self.extract_users(json_path)
        raw_prods = self.extract_products(xml_path)
        
        clean_records = self.transform(raw_txns, raw_users, raw_prods)
        self.load(clean_records)
        logger.info("ETL pipeline operations finalized cleanly.")

if __name__ == "__main__":
    pipeline = EcommerceETL()
    pipeline.run_pipeline("data/raw_transactions.csv", "data/user_metadata.json", "data/product_catalog.xml")