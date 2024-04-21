import unittest
from pipeline.functions import extract_non_numeric, extract_number


class TestExtractNonNumeric(unittest.TestCase):
    def test_extract_non_numeric(self):
        self.assertEquals('$', extract_non_numeric('$123'))
        self.assertEquals('$', extract_non_numeric('123$'))
        self.assertEquals('unit', extract_non_numeric('123'))
        self.assertEquals('$', extract_non_numeric('$12%3'))
        self.assertEquals('A', extract_non_numeric('A123'))
        self.assertEquals('$', extract_non_numeric('$123$'))

class TestExtractNumber(unittest.TestCase):
    def test_extract_non_numeric(self):
        self.assertEquals('123', extract_number('$123'))
        self.assertEquals('001', extract_number('001$'))
        self.assertEquals('123', extract_number('123'))
        self.assertEquals('123005', extract_number('$123,005'))
        self.assertEquals('123', extract_number('A123'))
        self.assertEquals('123', extract_number('$123$'))
        self.assertEquals('12.33', extract_number('$12.33'))


