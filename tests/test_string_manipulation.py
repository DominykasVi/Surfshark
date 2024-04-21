import unittest
from pipeline.functions import extract_non_numeric, extract_number


class TestExtractNonNumeric(unittest.TestCase):
    def test_extract_non_numeric(self):
        self.assertEqual('$', extract_non_numeric('$123'))
        self.assertEqual('$', extract_non_numeric('123$'))
        self.assertEqual('unit', extract_non_numeric('123'))
        self.assertEqual('$', extract_non_numeric('$12%3'))
        self.assertEqual('A', extract_non_numeric('A123'))
        self.assertEqual('$', extract_non_numeric('$123$'))

class TestExtractNumber(unittest.TestCase):
    def test_extract_non_numeric(self):
        self.assertEqual('123', extract_number('$123'))
        self.assertEqual('001', extract_number('001$'))
        self.assertEqual('123', extract_number('123'))
        self.assertEqual('123005', extract_number('$123,005'))
        self.assertEqual('123', extract_number('A123'))
        self.assertEqual('123', extract_number('$123$'))
        self.assertEqual('12.33', extract_number('$12.33'))


