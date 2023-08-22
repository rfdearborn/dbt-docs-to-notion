import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from urllib3.util.retry import Retry
from utils.request_util import make_request, get_paths_or_empty
import json
import re

DATABASE_PARENT_ID = os.environ['DATABASE_PARENT_ID']
DATABASE_NAME = os.environ['DATABASE_NAME']
NOTION_TOKEN = os.environ['NOTION_TOKEN']
MODEL_SELECT_METHOD = os.environ['MODEL_SELECT_METHOD']
MODEL_SELECT_REGEX = os.environ['MODEL_SELECT_REGEX']
MODEL_SELECT_LIST = os.environ['MODEL_SELECT_LIST']

def models_to_write(model_select_method, all_models_dict, model_select_list = [''], MODEL_SELECT_REGEX = ''):
    if model_select_method == 'select':
        sync_models_dict = model_select_list
    elif model_select_method == 'regex':
        print(f'{MODEL_SELECT_REGEX} string used for select')
        model_name_pattern = re.compile(MODEL_SELECT_REGEX)
        sync_models_dict = {
            node_name: data
            for (node_name, data) in all_models_dict.items()
            if model_name_pattern.match(data['name'])
        }
    else:
        sync_models_dict = all_models_dict
        
    sync_models_len = len(sync_models_dict)

    print(f'{ sync_models_len } selected for sync')
    return sync_models_dict, sync_models_len

def retry(exception, tries=10, delay=0.5, backoff=2):
    def decorator_retry(func):
        def wrapper(*args, **kwargs):
            retry_strategy = Retry(
                total=tries,
                backoff_factor=backoff,
                status_forcelist=[503],
                method_whitelist=["GET"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount("https://", adapter)

            for attempt in range(tries):
                try:
                    response = func(*args, **kwargs)
                    response.raise_for_status()
                    return response.json()
                except exception as e:
                    if attempt == tries - 1:
                        raise e
                    print(f"Retrying after {delay} seconds...")
                    time.sleep(delay)

        return wrapper

    return decorator_retry

def create_database():
    children_query_resp = make_request(
        endpoint='blocks/',
        querystring=f'{DATABASE_PARENT_ID}/children',
        method='GET'
    )

    database_id = ''
    for child in children_query_resp['results']:
        if 'child_database' in child and child['child_database'] == {'title': DATABASE_NAME}:
            database_id = child['id']
            break

    if database_id:
        print(f'database {database_id} already exists, proceeding to update records!')
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
                "Name": {"title": {}},
                "Description": {"rich_text": {}},
                "Owner": {"rich_text": {}},
                "Relation": {"rich_text": {}},
                "Approx Rows": {"number": {"format": "number_with_commas"}},
                "Approx GB": {"number": {"format": "number_with_commas"}},
                "Depends On": {"rich_text": {}},
                "Tags": {"rich_text": {}}
            }
        }

        print('creating database')
        database_creation_resp = make_request(
            endpoint='databases/',
            querystring='',
            method='POST',
            json=database_obj
        )
        database_id = database_creation_resp['id']
        print(f'\ncreated database {database_id}, proceeding to create records!')
    return database_id

def update_record(record_id, record_obj):
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    record_obj["properties"]["Docs Last Updated"] = {
        "rich_text": [{"text": {"content": current_datetime}}]
    }

    _record_update_resp = make_request(
        endpoint=f'pages/{record_id}',
        querystring='',
        method='PATCH',
        json=record_obj
    )

    # children can't be updated via record update, so we'll delete and re-add
    record_children_resp = make_request(
        endpoint='blocks/',
        querystring=f'{record_id}/children',
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
        querystring=f'{record_id}/children',
        method='PATCH',
        json={"children": record_obj['children']}
    )

def create_record(database_id, model_name, data, catalog_nodes):
    column_descriptions = {name: metadata['description'] for name, metadata in data['columns'].items()}
    col_names_and_data = list(get_paths_or_empty(catalog_nodes, [[model_name, 'columns']], {}).items())
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    columns_table_children_obj = [
        {
            "type": "table_row",
            "table_row": {
                "cells": [
                    [{"type": "text", "text": {"content": "Column"}, "plain_text": "Column"}],
                    [{"type": "text", "text": {"content": "Type"}, "plain_text": "Type"}],
                    [{"type": "text", "text": {"content": "Description"}, "plain_text": "Description"}]
                ]
            }
        }
    ]

    for (col_name, col_data) in col_names_and_data[:98]:
        column_description = column_descriptions.get(col_name.lower(), '')
        columns_table_children_obj.append(
            {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": col_name}, "plain_text": col_name}],
                        [{"type": "text", "text": {"content": col_data['type']}, "plain_text": col_data['type']}],
                        [{"type": "text", "text": {"content": column_description}, "plain_text": column_description}]
                    ]
                }
            }
        )

    if len(col_names_and_data) > 98:
        columns_table_children_obj.append(
            {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}],
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}],
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}]
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
                        "text": {"content": "Columns"}
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
                        "text": {"content": "Raw SQL"}
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
                            "content": data['raw_code'][:2000] if 'raw_code' in data else data['raw_sql'][:2000]
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
                        "text": {"content": "Compiled SQL"}
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
                            "content": data['compiled_code'][:2000] if 'compiled_code' in data else data['compiled_sql'][:2000]
                        }
                    }
                ],
                "language": "sql"
            }
        },
        # Last Updated
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Last Updated"}
                    }
                ]
            }
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": formatted_datetime}
                    }
                ]
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
                        }
                    }
                ]
            },
            "Owner": {
                "rich_text": [
                    {
                        "text": {
                            "content": str(get_owner(data, catalog_nodes, model_name))[:2000]
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
                "number": get_paths_or_empty(
                    catalog_nodes,
                    [[model_name, 'stats', 'num_rows', 'value'],
                     [model_name, 'stats', 'row_count', 'value']],
                    -1
                )
            },
            "Approx GB": {
                "number": get_paths_or_empty(
                    catalog_nodes,
                    [[model_name, 'stats', 'bytes', 'value'],
                     [model_name, 'stats', 'num_bytes', 'value']],
                    0
                ) / 1e9
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
        },
        "children": record_children_obj
    }

    query_obj = {
        "filter": {
            "property": "Name",
            "title": {
                "equals": data['name']
            }
        }
    }

    try:
        record_query_resp = make_request(
            endpoint=f'databases/{database_id}/query',
            querystring='',
            method='POST',
            json=query_obj
        )
    except json.JSONDecodeError as e:
        print(f'Skipping {model_name} due to JSON decode error:', e)
        return  # Skip this model_name and proceed to the next one


    if record_query_resp['results']:
        print(f'updating {model_name} record')
        record_id = record_query_resp['results'][0]['id']
        update_record(record_id, record_obj)

    else:
        print(f'creating {model_name} record')
        _record_creation_resp = make_request(
            endpoint='pages/',
            querystring='',
            method='POST',
            json=record_obj
        )

def get_owner(data, catalog_nodes, model_name):
  """
  Check for an owner field explicitly named in the DBT Config
  If none present, fall back to database table owner
  """
  owner = get_paths_or_empty(data, [['config', 'meta', 'owner']], None)
  if owner is not None:
    return owner

  return get_paths_or_empty(catalog_nodes, [[model_name, 'metadata', 'owner']], '')
  
def main():

    print(f'Sync started, in { MODEL_SELECT_METHOD } select mode')
    # Load nodes from dbt docs
    with open('target/manifest.json', encoding='utf-8') as f:
        manifest = json.load(f)
        manifest_nodes = manifest['nodes']

    with open('target/catalog.json', encoding='utf-8') as f:
        catalog = json.load(f)
        catalog_nodes = catalog['nodes']

    all_models_dict = {
        node_name: data
        for (node_name, data)
        in manifest_nodes.items() if data['resource_type'] == 'model'
    }

    all_models_len = len(all_models_dict)
    print(f'{ all_models_len } models in dbt project')

    sync_models_dict, sync_models_len = models_to_write(MODEL_SELECT_METHOD, all_models_dict, MODEL_SELECT_LIST, MODEL_SELECT_REGEX)

    # Create or update the database
    database_id = create_database()

    current_model_count = 0
    for model_name, data in sorted(sync_models_dict.items(), reverse=True):
        create_record(database_id, model_name, data, catalog_nodes)
        current_model_count = current_model_count + 1
        print(f'{current_model_count} models processed out of { sync_models_len }')

if __name__ == '__main__':
    main()
