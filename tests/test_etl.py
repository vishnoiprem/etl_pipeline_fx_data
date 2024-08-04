# test_etl.py

import unittest
import pandas as pd
from data_etl import load_data, validate_data, clean_data, transform_data

class TestETLFunctions(unittest.TestCase):

    def setUp(self):
        self.sample_data = pd.DataFrame({
            'datetime': ['2024-01-01 00:00:00', '2024-01-01 00:01:00'],
            'currency_pair': ['USDJPY', 'USDJPY'],
            'bid': [1.1, 1.2],
            'ask': [1.3, 1.4],
            'volume': [100, 200]
        })

    def test_load_data(self):
        df = load_data('../data/sample_fx_data_A.csv.gz')
        self.assertIsInstance(df, pd.DataFrame)

    def test_validate_data(self):
        df = validate_data(self.sample_data)
        self.assertFalse(df.isnull().values.any())

    def test_clean_data(self):
        df = clean_data(self.sample_data)
        self.assertIn('currency_pair', df.columns)
        self.assertEqual(df['currency_pair'][0], 'USD/JPY')

    def test_transform_data(self):
        df = transform_data(self.sample_data)
        self.assertIn('open', df.columns)
        self.assertIn('volume', df.columns)

if __name__ == '__main__':
    unittest.main()
