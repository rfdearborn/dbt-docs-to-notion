import json
import os
import requests
import sys
import time


DATABASE_PARENT_ID = os.environ['DATABASE_PARENT_ID']
DATABASE_NAME = os.environ['DATABASE_NAME']
NOTION_TOKEN = os.environ['NOTION_TOKEN']


def make_request(endpoint, querystring='', method='GET', **request_kwargs):
  time.sleep(0.34) # notion api limit is 3 requests per second

  headers = {
    'Authorization': NOTION_TOKEN,
    'Content-Type': 'application/json',
    'Notion-Version': '2022-02-22'
  }
  url = 'https://api.notion.com/v1/%s%s' % (endpoint, querystring)
  resp = requests.request(method, url, headers=headers, **request_kwargs)

  if not resp.status_code == 200:
    raise Exception(
      "Request returned status code %d\nResponse text: %s"
      % (resp.status_code, resp.text)
    )

  return resp.json()

def main():
  model_records_to_write = sys.argv[1:] # 'all' or list of model names
  print('Model records to write: %s' % model_records_to_write)

  ###### load nodes from dbt docs ######
  f = open('target/manifest.json', encoding='utf-8')
  manifest = json.load(f)
  manifest_nodes = manifest['nodes']

  f = open('target/catalog.json', encoding='utf-8')
  catalog = json.load(f)
  catalog_nodes = catalog['nodes']

  models = {node_name: data
        for (node_name, data)
        in manifest_nodes.items() if data['resource_type'] == 'model'}

  ###### create database if not exists ######
  children_query_resp = make_request(
    endpoint='blocks/',
    querystring='%s/children' % DATABASE_PARENT_ID,
    method='GET'
  )

  database_id = ''
  for child in children_query_resp['results']:
    if('child_database' in child
    and child['child_database'] == {'title': DATABASE_NAME}):
      database_id = child['id']
      break

  if database_id:
    print('database %s already exists, proceeding to update records!' % database_id)
  else:
    database_obj = {
      "title": [
        {
          "type": "text",
          "text": {
            "content": DATABASE_NAME,
            "link": None
          }
        }
      ],
      "parent": {
        "type": "page_id",
        "page_id": DATABASE_PARENT_ID
      },
      "properties": {
        "Name": {
          "title": {}
        },
        "Description": {
          "rich_text": {}
        },
        "Owner": {
          "rich_text": {}
        },
        "Relation": {
          "rich_text": {}
        },
        "Approx Rows": {
          "number": {
            "format": "number_with_commas"
          }
        },
        "Approx GB": {
          "number": {
            "format": "number_with_commas"
          }
        },
        "Depends On": {
          "rich_text": {}
        },
        "Tags": {
          "rich_text": {}
        }
      }
    }

    print('created creating database')
    database_creation_resp = make_request(
      endpoint='databases/',
      querystring='',
      method='POST',
      json=database_obj
    )
    database_id = database_creation_resp['id']
    print('\ncreated database %s, proceeding to create records!' % database_id)

  ##### create / update database records #####
  for model_name, data in sorted(list(models.items()), reverse=True):
    if model_records_to_write == ['all'] or model_name in model_records_to_write:
      # form record object
      column_descriptions = {name: metadata['description']
                  for name, metadata
                  in data['columns'].items()}

      columns_table_children_obj = [
        {
          "type": "table_row",
          "table_row": {
            "cells": [
              [
                {
                  "type": "text",
                  "text": {
                    "content": "Column"
                  },
                  "plain_text": "Column"
                }
              ],
              [
                {
                  "type": "text",
                  "text": {
                    "content": "Type"
                  },
                  "plain_text": "Type"
                }
              ],
              [
                {
                  "type": "text",
                  "text": {
                    "content": "Description"
                  },
                  "plain_text": "Description"
                }
              ]
            ]
          }
        }
      ]
      col_names_and_data = list(catalog_nodes[model_name]['columns'].items())
      for (col_name, col_data) in col_names_and_data[:98]: # notion api limit is 100 table rows
        columns_table_children_obj.append(
          {
            "type": "table_row",
            "table_row": {
              "cells": [
                [
                  {
                    "type": "text",
                    "text": {
                      "content": col_name
                    },
                    "plain_text": col_name
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": col_data['type']
                    },
                    "plain_text": col_data['type']
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": (
                        column_descriptions[col_name]
                        if col_name in column_descriptions
                        else ''
                      )
                    },
                    "plain_text": (
                      column_descriptions[col_name]
                      if col_name in column_descriptions
                      else ''
                    )
                  }
                ]
              ]
            }
          }
        )
      if len(col_names_and_data) > 98:
        # make that columns have been truncated
        columns_table_children_obj.append(
          {
            "type": "table_row",
            "table_row": {
              "cells": [
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "..."
                    },
                    "plain_text": "..."
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "..."
                    },
                    "plain_text": "..."
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "..."
                    },
                    "plain_text": "..."
                  }
                ]
              ]
            }
          }
        )

      record_children_obj = [
        # Table of contents
        {
          "object": "block",
          "type": "table_of_contents",
          "table_of_contents": {
            "color": "default"
          }
        },
        # Columns
        {
          "object": "block",
          "type": "heading_1",
          "heading_1": {
            "rich_text": [
              {
                "type": "text",
                "text": { "content": "Columns" }
              }
            ]
          }
        },
        {
          "object": "block",
          "type": "table",
          "table": {
            "table_width": 3,
            "has_column_header": True,
            "has_row_header": False,
            "children": columns_table_children_obj
          }
        },
        # Raw SQL
        {
          "object": "block",
          "type": "heading_1",
          "heading_1": {
            "rich_text": [
              {
                "type": "text",
                "text": { "content": "Raw SQL" }
              }
            ]
          }
        },
        {
          "object": "block",
          "type": "code",
          "code": {
            "rich_text": [
              {
                "type": "text",
                "text": {
                  "content": data['raw_sql'][:2000]
                }
              }
            ],
            "language": "sql"
          }
        },
        # Compiled SQL
        {
          "object": "block",
          "type": "heading_1",
          "heading_1": {
            "rich_text": [
              {
                "type": "text",
                "text": { "content": "Compiled SQL" }
              }
            ]
          }
        },
        {
          "object": "block",
          "type": "code",
          "code": {
            "rich_text": [
              {
                "type": "text",
                "text": {
                  "content": data['compiled_sql'][:2000]
                }
              }
            ],
            "language": "sql"
          }
        }
      ]

      record_obj = {
        "parent": {
          "database_id": database_id
        },
        "properties": {
          "Name": {
            "title": [
              {
                "text": {
                  "content": data['name']
                }
              }
            ]
          },
          "Description": {
            "rich_text": [
              {
                "text": {
                  "content": data['description'][:2000]
                  # notion api limit is 2k characters per rich text block
                }

              }
            ]
          },
          "Owner": {
            "rich_text": [
              {
                "text": {
                  "content": str(
                    catalog_nodes[model_name]['metadata']['owner']
                  )[:2000]
                }

              }
            ]
          },
          "Relation": {
            "rich_text": [
              {
                "text": {
                  "content": data['relation_name'][:2000]
                }

              }
            ]
          },
          "Approx Rows": {
            "number": catalog_nodes[model_name]['stats']['num_rows']['value']
          },
          "Approx GB": {
            "number": catalog_nodes[model_name]['stats']['num_bytes']['value']/1e9
          },
          "Depends On": {
            "rich_text": [
              {
                "text": {
                  "content": json.dumps(data['depends_on'])[:2000]
                }

              }
            ]
          },
          "Tags": {
            "rich_text": [
              {
                "text": {
                  "content": json.dumps(data['tags'])[:2000]
                }

              }
            ]
          }
        }
      }

      ###### query to see if record already exists ######
      query_obj = {
        "filter": {
          "property": "Name",
          "title": {
            "equals": data['name']
          }
        }
      }
      record_query_resp = make_request(
        endpoint='databases/',
        querystring='%s/query' % database_id,
        method='POST',
        json=query_obj
      )

      if record_query_resp['results']:
        print('\nupdating %s record' % model_name)
        record_id = record_query_resp['results'][0]['id']
        _record_update_resp = make_request(
          endpoint='pages/%s' % record_id,
          querystring='',
          method='PATCH',
          json=record_obj
        )

        # children can't be updated via record update, so we'll delete and re-add
        record_children_resp = make_request(
          endpoint='blocks/',
          querystring='%s/children' % record_id,
          method='GET'
        )
        for record_child in record_children_resp['results']:
          record_child_id = record_child['id']
          _record_child_deletion_resp = make_request(
            endpoint='blocks/',
            querystring=record_child_id,
            method='DELETE'
          )

        _record_children_replacement_resp = make_request(
          endpoint='blocks/',
          querystring='%s/children' % record_id,
          method='PATCH',
          json={"children": record_children_obj}
        )

      else:
        print('\ncreating %s record' % model_name)
        record_obj['children'] = record_children_obj
        _record_creation_resp = make_request(
          endpoint='pages/',
          querystring='',
          method='POST',
          json=record_obj
        )

if __name__ == '__main__':
  main()