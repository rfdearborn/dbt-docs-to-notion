import requests
import time
import os


NOTION_TOKEN = os.environ['NOTION_TOKEN']

def make_request(endpoint, querystring='', method='GET', **request_kwargs):
    time.sleep(0.5)  # Rate limit: 3 requests per second
    headers = {
        'Authorization': f'Bearer {NOTION_TOKEN}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-02-22'
    }
    url = f'https://api.notion.com/v1/{endpoint}{querystring}'
    resp = requests.request(method, url, headers=headers, **request_kwargs)
    return resp.json()


def get_paths_or_empty(parent_object, paths_array, zero_value=''):
  """Used for catalog_nodes accesses, since structure is variable"""
  for path in paths_array:
    obj = parent_object
    for el in path:
      if el not in obj:
        obj = zero_value
        break
      obj = obj[el]
    if obj != zero_value:
      return obj

  return zero_value