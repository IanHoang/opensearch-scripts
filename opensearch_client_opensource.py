from opensearchpy import OpenSearch
import opensearchpy

from exceptions import CreateIndexError, SystemSetupError
from ingestion_prepper import write_to_string

import logging

logger = logging.getLogger(__name__)

host = '' # OpenSource Endpoint
auth = ('admin', 'admin') # For testing only. Don't store credentials in code.
port = 443

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
)

# Create an index with non-default settings.
index_name = 'python-test-index-test'
index_body = {
  'settings': {
    'index': {
      'number_of_shards': 5
    }
  }
}

def create_index(index_name:str, index_body:dict):
  try:
    response = client.indices.create(index_name, body=index_body)
  except opensearchpy.exceptions.RequestError:
    msg = f"Could not create index {index_name}!"
    raise CreateIndexError(msg)


# https://opensearch-project.github.io/opensearch-py/_modules/opensearchpy/exceptions.html
def validating_mapping_and_create_monthly_index(index_name_with_date:str, index_mappings:str):
  try:
    response = client.indices.create(index_name_with_date, body=index_mappings)
    print('\nCreating index:')
    print(response)
  except opensearchpy.exceptions.RequestError:
    msg = f"Could not create index {index_name_with_date}"
    raise CreateIndexError(msg)

# validating_mapping_and_create_monthly_index("index-3-24", index_body)
# create_index("grocery-products", index_body)
# products = write_to_string()
# print(products)
# client.bulk(products)
query = {
    "size": 5,
    "query": {
        "match_all": {}
    }
}
response = client.search(
  body=query,
  index='grocery-products'
)

print(response)