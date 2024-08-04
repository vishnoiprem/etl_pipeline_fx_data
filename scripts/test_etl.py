import unittest
import pandas as pd
from data_etl import load_data, validate_data, clean_data, transform_data

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

    def test_load_data(self):
        # Use the correct path to your data file
        df = load_data('../data/sample_fx_data_B.csv.gz')
        self.assertIsInstance(df, pd.DataFrame)

    def test_validate_data(self):
        with self.assertRaises(ValueError):
            validate_data(pd.DataFrame())

        # Test with valid data
        valid_df = validate_data(self.df)
        self.assertFalse(valid_df.empty)

    def test_clean_data(self):
        df = clean_data(self.df)
        self.assertEqual(len(df), 2)
        self.assertTrue(all(df['currency_pair'].isin(['USDJPY', 'EURJPY'])))

    def test_transform_data(self):
        df = transform_data(self.df)
        self.assertEqual(df.shape[1], 6)
        self.assertTrue('currency_pair' in df.columns)
        self.assertEqual(df['currency_pair'].iloc[0], 'USD/JPY')

if __name__ == '__main__':
    unittest.main()