import pytest
from etl_pipeline import EcommerceETL

def test_transformation_logic_filters():
    etl = EcommerceETL(db_path=":memory:") # Use isolated volatile in-memory DB configuration
    
    mock_raw_txns = [
        {"transaction_id": "T1", "user_id": "U1", "product_id": "P1", "amount": "100.00", "currency": "USD", "timestamp": "2026-06-29T12:00:00Z"},
        {"transaction_id": "T2", "user_id": "U2", "product_id": "P2", "amount": "-50.00", "currency": "USD", "timestamp": "2026-06-29T12:00:00Z"}, # Bad: Negative value
        {"transaction_id": "T3", "user_id": "U1", "product_id": "P3", "amount": "", "currency": "EUR", "timestamp": "2026-06-29T12:00:00Z"}      # Bad: Empty value
    ]
    
    mock_user_map = {
        "U1": {"user_id": "U1", "signup_country": "UK", "is_premium": True}
    }

    result = etl.transform(mock_raw_txns, mock_user_map)
    
    # Assert checks: Verify both bad rows were correctly excluded by transformation guards
    assert len(result) == 1
    assert result[0][0] == "T1"         # Transaction ID mapping match
    assert result[0][3] == 100.00       # Parsed amount validation metric
    assert result[0][4] == "UK"         # Enriched profile attribute mapped matching profile database