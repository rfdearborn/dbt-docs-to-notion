import unittest
from unittest.mock import patch, Mock

from dbt_docs_to_notion import make_request, get_paths_or_empty, get_owner
from tests.mock_data import DBT_MOCK_MANIFEST, DBT_MOCK_CATALOG, NOTION_MOCK_DATABASE_CREATE


class TestMakeRequest(unittest.TestCase):
    @patch('dbt_docs_to_notion.requests.request')
    def test_valid_request(self, mock_request):
        mock_request.return_value = Mock(status_code=200, json=lambda: NOTION_MOCK_DATABASE_CREATE)
        response = make_request("some_endpoint")
        self.assertEqual(response, NOTION_MOCK_DATABASE_CREATE)

    @patch('dbt_docs_to_notion.requests.request')
    def test_invalid_token(self, mock_request):
        mock_request.return_value = Mock(status_code=403, json=lambda: {"message": "Invalid token"})
        with self.assertRaises(Exception) as context:
            make_request("some_endpoint")
        self.assertIn("Request returned status code 403", str(context.exception))

    @patch('dbt_docs_to_notion.requests.request')
    def test_error_response(self, mock_request):
        mock_request.return_value = Mock(status_code=500, json=lambda: {"message": "Server error"})
        with self.assertRaises(Exception) as context:
            make_request("some_endpoint")
        self.assertIn("Request returned status code 500", str(context.exception))


class TestGetPathsOrEmpty(unittest.TestCase):
    def test_valid_path(self):
        result = get_paths_or_empty(DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"], [["description"]])
        self.assertEqual(result, "Description for model 1")

    def test_invalid_path(self):
        result = get_paths_or_empty(DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"], [["invalid_path"]])
        self.assertEqual(result, '')


class TestGetOwner(unittest.TestCase):
    def test_owner_in_config(self):
        data = DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"]
        catalog_nodes = DBT_MOCK_CATALOG["nodes"]
        result = get_owner(data, catalog_nodes, "model.test.model_1")
        self.assertEqual(result, "owner@example.com")


if __name__ == '__main__':
    unittest.main()
