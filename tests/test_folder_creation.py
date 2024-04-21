import unittest
from pipeline.functions import check_and_create_folder
import os
import shutil

class TestCreateFolder(unittest.TestCase):
    def test_extract_non_numeric(self):
        check_and_create_folder('tests/test_folders/')
        self.assertTrue(os.path.exists('tests/test_folders/')) 

        check_and_create_folder('tests/test_folders/test1/')
        self.assertTrue(os.path.exists('tests/test_folders/test1/')) 

        check_and_create_folder('tests/test_folders/test2/file.txt')
        self.assertTrue(os.path.exists('tests/test_folders/test2/'))

        check_and_create_folder('tests/test_folders/test3/test4')
        self.assertTrue(os.path.exists('tests/test_folders/test3')) 
        self.assertFalse(os.path.exists('tests/test_folders/test3/test4')) 

        try:
            shutil.rmtree('tests/test_folders/')
        except Exception as e:
            print(f"Failed to delete test folders because of {str(e)}")
