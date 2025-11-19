import pandas as pd
from elasticsearch import Elasticsearch, helpers
import json

# Elasticsearch connection config - replace these with your details
ES_HOST = 'https://healthcareone-d7bc53.es.us-central1.gcp.elastic.cloud:443'
API_KEY = 'MkxLOGQ1b0JBMEtmVDF5a0Z1Z086Y1VtelhhYTBWM1cyX2pWaXRKdWJrdw=='

# Connect to Elasticsearch cluster using API key
es = Elasticsearch(
    [ES_HOST],
    api_key=API_KEY,
    verify_certs=True
)

# Read Excel sheets
excel_file = 'HealthCareOne_Extended_Operational_Data.xlsx'
summary_df = pd.read_excel(excel_file, sheet_name='Sheet1')



# Define index names
summary_index = 'healthcareone_summary'

# Helper generator function for bulk upload
def generate_actions(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": json.loads(row.to_json())
        }

# Optionally delete existing indices (be cautious in production)
if es.indices.exists(index=summary_index):
    es.indices.delete(index=summary_index)


# Bulk index the data
helpers.bulk(es, generate_actions(summary_df, summary_index))

print("Data indexed successfully.")
print(f"Summary index count: {es.count(index=summary_index)['count']}")

