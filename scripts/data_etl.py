import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    logging.info(f'Loading data from {file_path}')
    try:
        return pd.read_csv(file_path, compression='gzip')
    except Exception as e:
        logging.error(f'Error loading data: {e}')
        raise

def validate_data(df):
    logging.info('Validating data')
    # Example validation: drop rows with any NaN values
    return df.dropna()

def clean_data(df):
    logging.info('Cleaning data')
    # Convert currency_pair to XXX/YYY format
    df['currency_pair'] = df['currency_pair'].apply(lambda x: f"{x[:3]}/{x[3:]}")
    return df

def transform_data(df):
    logging.info('Transforming data')
    df = df.copy()
    df['mid_price'] = (df['bid'] + df['ask']) / 2
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    # Resample to minute intervals using 'min'
    minutely_data = df.resample('min').agg({
        'mid_price': ['first', 'max', 'min', 'last'],
        'volume': 'sum'
    })

    minutely_data.columns = ['open', 'high', 'low', 'close', 'volume']
    minutely_data.reset_index(inplace=True)

    return minutely_data

def save_data(df, output_path):
    logging.info(f'Saving data to {output_path}')
    try:
        df.to_csv(output_path, index=False, compression='gzip')
    except Exception as e:
        logging.error(f'Error saving data: {e}')
        raise

if __name__ == "__main__":
    input_file = '../data/sample_fx_data_A.csv.gz'  # Change to the chosen data provider
    output_file = '../output/minutely_data.csv.gz'

    try:
        data = load_data(input_file)
        validated_data = validate_data(data)
        cleaned_data = clean_data(validated_data)
        # Filter for specific currency pairs and date
        filtered_data = cleaned_data[(cleaned_data['currency_pair'].isin(['USD/JPY', 'EUR/JPY'])) & (cleaned_data['datetime'] >= '2024-01-01')]
        transformed_data = transform_data(filtered_data)
        save_data(transformed_data, output_file)
        logging.info('ETL process completed successfully')
    except Exception as e:
        logging.error(f'ETL process failed: {e}')
