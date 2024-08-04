import unittest
import pandas as pd
from data_etl import load_data, validate_data, clean_data, transform_data
import os

class TestETL(unittest.TestCase):
    def setUp(self):
        self.sample_data = {
            'datetime': ['2024-01-01 00:00:01', '2024-01-01 00:00:02'],
            'currency_pair': ['USDJPY', 'EURJPY'],
            'bid': [108.50, 117.25],
            'ask': [108.55, 117.30],
            'volume': [100, 150]
        }
        self.df = pd.DataFrame(self.sample_data)
        self.sample_data_file = '../data/sample_fx_data_B.csv.gz'

    def test_load_data(self):
        # Ensure the file path is correct
        if os.path.exists(self.sample_data_file):
            df = load_data(self.sample_data_file)
            self.assertIsInstance(df, pd.DataFrame)
        else:
            self.skipTest(f"Test skipped because {self.sample_data_file} does not exist")

    def test_validate_data(self):
        with self.assertRaises(ValueError):
            validate_data(pd.DataFrame())

    def test_clean_data(self):
        df = clean_data(self.df)
        self.assertEqual(len(df), 2)
        self.assertTrue(all(df['currency_pair'].isin(['USDJPY', 'EURJPY'])))

    def test_transform_data(self):
        df = transform_data(self.df)
        # There should be 6 columns: datetime, open, high, low, close, volume
        self.assertEqual(df.shape[1], 6)
        # Check if the first row's currency_pair column is 'USD/JPY'
        self.assertEqual(df['currency_pair'].iloc[0], 'USD/JPY')

if __name__ == '__main__':
    unittest.main()