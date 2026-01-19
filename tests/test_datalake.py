import os
import json
import shutil
import unittest
from datetime import datetime
from src import datalake

class TestDataLake(unittest.TestCase):
    def setUp(self):
        self.test_base_path = "test_data_lake"
        os.makedirs(self.test_base_path, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.test_base_path):
            shutil.rmtree(self.test_base_path)

    def test_ensure_dir(self):
        path = os.path.join(self.test_base_path, "subdir")
        datalake.ensure_dir(path)
        self.assertTrue(os.path.exists(path))

    def test_write_channel_messages_json(self):
        date_str = "2024-01-01"
        channel_name = "test_channel"
        messages = [{"id": 1, "text": "hello"}]
        
        path = datalake.write_channel_messages_json(
            base_path=self.test_base_path,
            date_str=date_str,
            channel_name=channel_name,
            messages=messages
        )
        
        self.assertTrue(os.path.exists(path))
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(data, messages)

    def test_write_manifest(self):
        date_str = "2024-01-01"
        counts = {"test_channel": 1}
        
        path = datalake.write_manifest(
            base_path=self.test_base_path,
            date_str=date_str,
            channel_message_counts=counts
        )
        
        self.assertTrue(os.path.exists(path))
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(data["total_messages"], 1)
            self.assertEqual(data["channels"], counts)

if __name__ == "__main__":
    unittest.main()
