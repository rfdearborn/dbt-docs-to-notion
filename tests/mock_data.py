# Mock Data for dbt and Notion API
import os

# Mock dbt Data
DBT_MOCK_CATALOG = {
  "nodes": {
    "model.test.model_1": {
      "columns": {
        "column_1": {
          "type": "TEXT"
        },
        "column_2": {
          "type": "TEXT"
        },
      },
      "metadata": {
        "owner": "owner@example.com"
      },
      "stats": {
        "row_count": {
          "value": 1,
        },
        "bytes": {
          "value": 1000000,
        },
      },
    },
  },
}

DBT_MOCK_MANIFEST = {
  "nodes": {
    "model.test.model_1": {
      "resource_type": "model",
      "columns": {
        "column_1": {
          "description": "Description for column 1"
        },
        "column_2": {
          "description": "Description for column 2"
        },
      },
      "raw_code": "SELECT 1",
      "compiled_code": "SELECT 1",
      "name": "model_1",
      "description": "Description for model 1",
      "relation_name": "model.test.model_1",
      "depends_on": ["model.test.model_2"],
      "tags": ["tag1", "tag2"],
    },
  },
}

DBT_MOCK_CATALOG_MULTI = {
  "nodes": {
    "model.test.model_1": DBT_MOCK_CATALOG["nodes"]["model.test.model_1"],
    "model.test.model_2": {
      "columns": {},
      "metadata": {"owner": "owner2@example.com"},
      "stats": {
        "row_count": {"value": 5},
        "bytes": {"value": 2000000},
      },
    },
  },
}

DBT_MOCK_MANIFEST_MULTI = {
  "nodes": {
    "model.test.model_1": DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"],
    "model.test.model_2": {
      "resource_type": "model",
      "columns": {},
      "raw_code": "SELECT 2",
      "compiled_code": "SELECT 2",
      "name": "model_2",
      "description": "Description for model 2",
      "relation_name": "model.test.model_2",
      "depends_on": [],
      "tags": [],
    },
    "test.test.test_1": {
      "resource_type": "test",
      "name": "test_1",
    },
  },
}

# Mock Notion API Responses
NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY = {
  "results": [
    {
      "id": "mock_child_id",
      "child_database": {
        "title": os.environ['DATABASE_NAME'],
      },
    },
  ],
}

NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY = {
  "results": [
    {
      "id": "mock_record_id",
    },
  ],
}

NOTION_MOCK_NONEXISTENT_QUERY = {
  "results": [],
}

NOTION_MOCK_DATABASE_CREATE = {
  "id": "mock_database_id",
}

NOTION_MOCK_RECORD_CREATE = {
  "id": "mock_record_id",
}
