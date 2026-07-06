import pandas as pd
from sqlalchemy import create_engine, text
import pymongo

print('='*70)
print('PROCESS 1: PREPROCESSED & FEATURE-ENGINEERED DATA')
print('='*70)

print('\n--- TEXT (PubMed, with features) ---')
df = pd.read_parquet('processed/pubmed_processed.parquet')
print(df[['pmid', 'ingredient_term', 'abstract_length']].head(5))

print('\n--- IMAGES (OCR + risk features) ---')
df = pd.read_parquet('processed/images_processed.parquet')
print(df[['product_code', 'width', 'height', 'ocr_text']].head(5))

print('\n--- AUDIO (MFCC features) ---')
df = pd.read_parquet('processed/audio_processed.parquet')
print(df[['file', 'duration_sec', 'used_whisper']].head(5))

print('\n--- STRUCTURED (CosIng, cleaned) ---')
df = pd.read_parquet('processed/cosing_processed.parquet')
print(df[['inci_name_std', 'function', 'regulatory_status']].head(5))

print('\n' + '='*70)
print('PROCESS 2: ETL - FINAL LOADED DATABASE STATE')
print('='*70)

engine = create_engine('postgresql+psycopg2://postgres:Subhi%40090904@localhost:5432/skincare_staging')

print('\n--- PostgreSQL: adverse_events (sample) ---')
print(pd.read_sql(text('SELECT safetyreportid, ingredient_term, receivedate FROM adverse_events LIMIT 5'), engine))

print('\n--- PostgreSQL: cosing_staging (sample) ---')
print(pd.read_sql(text('SELECT inci_name, function, regulatory_status FROM cosing_staging LIMIT 5'), engine))

print('\n--- PostgreSQL: pubmed_evidence (sample) ---')
print(pd.read_sql(text('SELECT pmid, ingredient_term, abstract_length FROM pubmed_evidence LIMIT 5'), engine))

print('\n--- PostgreSQL: row counts per table ---')
print(pd.read_sql(text('SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC'), engine))

print('\n--- MongoDB: openbeautyfacts_raw (sample) ---')
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['skincare_raw']
for doc in db['openbeautyfacts_raw'].find().limit(3):
    print({k: doc[k] for k in list(doc.keys())[:5]})