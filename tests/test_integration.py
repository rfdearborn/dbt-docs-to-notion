import json
import os
import unittest
from unittest.mock import patch, Mock

from dbt_docs_to_notion import get_owner, get_paths_or_empty, main
from tests.mock_data import (
  DBT_MOCK_CATALOG,
  DBT_MOCK_CATALOG_MULTI,
  DBT_MOCK_MANIFEST,
  DBT_MOCK_MANIFEST_MULTI,
  NOTION_MOCK_DATABASE_CREATE,
  NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY,
  NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY,
  NOTION_MOCK_NONEXISTENT_QUERY,
  NOTION_MOCK_RECORD_CREATE,
)


class TestGetPathsOrEmpty(unittest.TestCase):

    def test_first_path_matches(self):
        obj = {'a': {'b': 'value'}}
        result = get_paths_or_empty(obj, [['a', 'b']])
        self.assertEqual(result, 'value')

    def test_first_path_misses_second_matches(self):
        obj = {'stats': {'row_count': {'value': 42}}}
        result = get_paths_or_empty(obj, [['stats', 'num_rows', 'value'], ['stats', 'row_count', 'value']])
        self.assertEqual(result, 42)

    def test_all_paths_miss(self):
        obj = {'a': 1}
        result = get_paths_or_empty(obj, [['x', 'y'], ['z']])
        self.assertEqual(result, '')

    def test_custom_zero_value(self):
        obj = {'a': 1}
        result = get_paths_or_empty(obj, [['x']], zero_value=-1)
        self.assertEqual(result, -1)


class TestGetOwner(unittest.TestCase):

    def test_config_meta_owner_present(self):
        data = {'config': {'meta': {'owner': 'team-a'}}}
        catalog_nodes = {'model.test.m': {'metadata': {'owner': 'db-owner'}}}
        self.assertEqual(get_owner(data, catalog_nodes, 'model.test.m'), 'team-a')

    def test_falls_back_to_catalog_owner(self):
        data = {'config': {'meta': {}}}
        catalog_nodes = {'model.test.m': {'metadata': {'owner': 'db-owner'}}}
        self.assertEqual(get_owner(data, catalog_nodes, 'model.test.m'), 'db-owner')

    def test_both_absent(self):
        data = {}
        catalog_nodes = {}
        self.assertEqual(get_owner(data, catalog_nodes, 'model.test.m'), '')


class TestDbtDocsToNotionIntegration(unittest.TestCase):

    def setUp(self):
        patch('dbt_docs_to_notion.json.load').start().side_effect = [DBT_MOCK_MANIFEST, DBT_MOCK_CATALOG]
        self.mock_open = patch('dbt_docs_to_notion.open', new_callable=unittest.mock.mock_open, read_data="data").start()
        self.comparison_catalog = DBT_MOCK_CATALOG['nodes']['model.test.model_1']
        self.comparison_manifest = DBT_MOCK_MANIFEST['nodes']['model.test.model_1']
        self.recorded_requests = []

    def tearDown(self):
        patch.stopall()

    def _verify_database_obj(self, database_obj):
      title = database_obj['title'][0]
      self.assertEqual(title['type'], 'text')
      self.assertEqual(title['text']['content'], os.environ['DATABASE_NAME'])
      parent = database_obj['parent']
      self.assertEqual(parent['type'], 'page_id')
      self.assertEqual(parent['page_id'], os.environ['DATABASE_PARENT_ID'])
      properties = database_obj['properties']
      self.assertEqual(properties['Name'], {'title': {}})
      self.assertEqual(properties['Description'], {'rich_text': {}})
      self.assertEqual(properties['Owner'], {'rich_text': {}})
      self.assertEqual(properties['Relation'], {'rich_text': {}})
      self.assertEqual(
        properties['Approx Rows'],
        {'number': {'format': 'number_with_commas'}}
      )
      self.assertEqual(
        properties['Approx GB'],
        {'number': {'format': 'number_with_commas'}}
      )
      self.assertEqual(properties['Depends On'], {'rich_text': {}})
      self.assertEqual(properties['Tags'], {'rich_text': {}})

    def _verify_record_obj(self, record_obj):
      parent = record_obj['parent']
      self.assertEqual(parent['database_id'], NOTION_MOCK_DATABASE_CREATE['id'])
      properties = record_obj['properties']
      self.assertEqual(properties['Name']['title'][0]['text']['content'], self.comparison_manifest['name'])
      self.assertEqual(properties['Description']['rich_text'][0]['text']['content'], self.comparison_manifest['description'])
      self.assertEqual(properties['Owner']['rich_text'][0]['text']['content'], self.comparison_catalog['metadata']['owner'])
      self.assertEqual(properties['Relation']['rich_text'][0]['text']['content'], self.comparison_manifest['relation_name'])
      self.assertEqual(properties['Approx Rows']['number'], self.comparison_catalog['stats']['row_count']['value'])
      self.assertEqual(properties['Approx GB']['number'], self.comparison_catalog['stats']['bytes']['value']/1e9)
      self.assertEqual(properties['Depends On']['rich_text'][0]['text']['content'], json.dumps(self.comparison_manifest['depends_on']))
      self.assertEqual(properties['Tags']['rich_text'][0]['text']['content'], json.dumps(self.comparison_manifest['tags']))

    def _verify_record_children_obj(self, record_children_obj):
      toc_child_block = record_children_obj[0]
      self.assertEqual(toc_child_block['object'], 'block')
      self.assertEqual(toc_child_block['type'], 'table_of_contents')
      columns_header_child_block = record_children_obj[1]
      self.assertEqual(columns_header_child_block['object'], 'block')
      self.assertEqual(columns_header_child_block['type'], 'heading_1')
      self.assertEqual(columns_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Columns')
      columns_child_block = record_children_obj[2]
      self.assertEqual(columns_child_block['object'], 'block')
      self.assertEqual(columns_child_block['type'], 'table')
      self.assertEqual(columns_child_block['table']['table_width'], 3)
      self.assertEqual(columns_child_block['table']['has_column_header'], True)
      self.assertEqual(columns_child_block['table']['has_row_header'], False)
      columns_table_children_obj = columns_child_block['table']['children']
      columns_table_header_row = columns_table_children_obj[0]
      self.assertEqual(columns_table_header_row['type'], 'table_row')
      self.assertEqual(columns_table_header_row['table_row']['cells'][0][0]['plain_text'], 'Column')
      self.assertEqual(columns_table_header_row['table_row']['cells'][1][0]['plain_text'], 'Type')
      self.assertEqual(columns_table_header_row['table_row']['cells'][2][0]['plain_text'], 'Description')
      columns_table_row = columns_table_children_obj[1]
      self.assertEqual(columns_table_row['type'], 'table_row')
      self.assertEqual(columns_table_row['table_row']['cells'][0][0]['plain_text'], list(self.comparison_catalog['columns'].keys())[0])
      self.assertEqual(columns_table_row['table_row']['cells'][1][0]['plain_text'], list(self.comparison_catalog['columns'].values())[0]['type'])
      self.assertEqual(columns_table_row['table_row']['cells'][2][0]['plain_text'], list(self.comparison_manifest['columns'].values())[0]['description'])
      raw_code_header_child_block = record_children_obj[3]
      self.assertEqual(raw_code_header_child_block['object'], 'block')
      self.assertEqual(raw_code_header_child_block['type'], 'heading_1')
      self.assertEqual(raw_code_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Raw Code')
      raw_code_child_block = record_children_obj[4]
      self.assertEqual(raw_code_child_block['object'], 'block')
      self.assertEqual(raw_code_child_block['type'], 'code')
      self.assertEqual(raw_code_child_block['code']['language'], 'sql')
      self.assertEqual(raw_code_child_block['code']['rich_text'][0]['text']['content'], self.comparison_manifest['raw_code'])
      compiled_code_header_child_block = record_children_obj[5]
      self.assertEqual(compiled_code_header_child_block['object'], 'block')
      self.assertEqual(compiled_code_header_child_block['type'], 'heading_1')
      self.assertEqual(compiled_code_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Compiled Code')
      compiled_code_child_block = record_children_obj[6]
      self.assertEqual(compiled_code_child_block['object'], 'block')
      self.assertEqual(compiled_code_child_block['type'], 'code')
      self.assertEqual(compiled_code_child_block['code']['language'], 'sql')
      self.assertEqual(compiled_code_child_block['code']['rich_text'][0]['text']['content'], self.comparison_manifest['compiled_code'])

    @patch('dbt_docs_to_notion.make_request')
    def test_create_new_database(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring, method, **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'databases/' and querystring == '' and method == 'POST':
              database_obj = request_kwargs['json']
              self._verify_database_obj(database_obj)
              return NOTION_MOCK_DATABASE_CREATE
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'pages/' and method == 'POST':
              record_obj = request_kwargs['json']
              self._verify_record_obj(record_obj)
              record_children_obj = request_kwargs['json']['children']
              self._verify_record_children_obj(record_children_obj)
              return NOTION_MOCK_RECORD_CREATE
        mock_make_request.side_effect = _mocked_make_request

        main(argv=[None, 'dbt_project_dir', 'all'])

        self.mock_open.assert_any_call('dbt_project_dir/target/manifest.json', encoding='utf-8')
        self.mock_open.assert_any_call('dbt_project_dir/target/catalog.json', encoding='utf-8')
        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'POST'),
            ('databases/', 'POST'),
            ('pages/', 'POST'),
          ]
        )

    @patch('dbt_docs_to_notion.make_request')
    def test_update_existing_database(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring, method, **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY
          elif endpoint == 'pages/' and method == 'PATCH':
              record_obj = request_kwargs['json']
              self._verify_record_obj(record_obj)
              return {} # response is thrown away
          elif endpoint == 'blocks/' and method == 'DELETE':
              return {} # response is thrown away
          elif endpoint == 'blocks/' and method == 'PATCH':
              record_children_obj = request_kwargs['json']['children']
              self._verify_record_children_obj(record_children_obj)
              return {} # response is thrown away
        mock_make_request.side_effect = _mocked_make_request

        main(argv=[None, 'dbt_project_dir', 'all'])

        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'POST'),
            ('pages/mock_record_id', 'PATCH'),
            ('blocks/', 'GET'),
            ('blocks/', 'DELETE'),
            ('blocks/', 'PATCH'),
          ]
        )


    @patch('dbt_docs_to_notion.make_request')
    def test_backward_compat_without_project_dir(self, mock_make_request):
        """Test that the old calling convention (no project dir) still works."""
        def _mocked_make_request(endpoint, querystring, method, **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'databases/' and querystring == '' and method == 'POST':
              return NOTION_MOCK_DATABASE_CREATE
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'pages/' and method == 'POST':
              return NOTION_MOCK_RECORD_CREATE
        mock_make_request.side_effect = _mocked_make_request

        main(argv=[None, 'all'])

        self.mock_open.assert_any_call('./target/manifest.json', encoding='utf-8')
        self.mock_open.assert_any_call('./target/catalog.json', encoding='utf-8')

    @patch('dbt_docs_to_notion.make_request')
    def test_filter_specific_model(self, mock_make_request):
        """Test that passing specific model names only processes those models,
        and that non-model nodes (e.g. tests) are filtered out."""
        patch.stopall()
        patch('dbt_docs_to_notion.json.load').start().side_effect = [DBT_MOCK_MANIFEST_MULTI, DBT_MOCK_CATALOG_MULTI]
        patch('dbt_docs_to_notion.open', new_callable=unittest.mock.mock_open, read_data="data").start()

        created_models = []
        def _mocked_make_request(endpoint, querystring, method, **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'databases/' and querystring == '' and method == 'POST':
              return NOTION_MOCK_DATABASE_CREATE
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'pages/' and method == 'POST':
              name = request_kwargs['json']['properties']['Name']['title'][0]['text']['content']
              created_models.append(name)
              return NOTION_MOCK_RECORD_CREATE
        mock_make_request.side_effect = _mocked_make_request

        # Use short name 'model_1' — tests PR #28's split(".")[-1] matching
        main(argv=[None, 'mydir', 'model_1'])

        self.assertEqual(created_models, ['model_1'])


if __name__ == '__main__':
    unittest.main()
