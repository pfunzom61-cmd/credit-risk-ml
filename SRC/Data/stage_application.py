import pandas as pd
from sqlalchemy import create_engine, text

# 1. Config
DB_URL = "postgresql://postgres:PfunzoPostgres@localhost:5432/credit_risk"
FILE_PATH = "Data/raw/application_train.csv"

def run_full_staging():
    engine = create_engine(DB_URL)
    
    print("Connecting to Database...")
    with engine.connect() as conn:
        # TRUNCATE ensures we don't have duplicates from our smoke test
        print("Cleaning table for full load...")
        conn.execute(text("TRUNCATE TABLE application_train;"))
        conn.commit()

    print("Reading CSV (Streamed)...")
    # We use chunksize to process 50,000 rows at a time
    # This is how banks handle millions of rows efficiently
    for i, chunk in enumerate(pd.read_csv(FILE_PATH, chunksize=50000)):
        chunk.columns = [c.lower() for c in chunk.columns]
        
        # Ingest the chunk
        chunk.to_sql('application_train', engine, if_exists='append', index=False)
        print(f"Batch {i+1}: Ingested {len(chunk)} rows...")

    print("FULL STAGING COMPLETE.")

if __name__ == "__main__":
    run_full_staging()