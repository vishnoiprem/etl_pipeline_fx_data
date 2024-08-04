import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)

def load_data(file_path):
    try:
        logging.info(f"Loading data from {file_path}")
        return pd.read_csv(file_path, compression='gzip')
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def validate_data(df):
    if df.empty:
        raise ValueError("DataFrame is empty")
    logging.info("Validating data")
    return df.dropna()

def clean_data(df):
    logging.info("Cleaning data")
    return df

def transform_data(df):
    logging.info("Transforming data")
    df = df.copy()
    df['mid_price'] = (df['bid'] + df['ask']) / 2
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    minutely_data = df.resample('min').agg({
        'mid_price': ['first', 'max', 'min', 'last'],
        'volume': 'sum'
    })

    minutely_data.columns = ['open', 'high', 'low', 'close', 'volume']
    minutely_data.reset_index(inplace=True)
    minutely_data['currency_pair'] = 'USD/JPY'

    return minutely_data

def save_data(df, output_path):
    try:
        logging.info(f"Saving data to {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, compression='gzip')
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        raise

if __name__ == "__main__":
    file_list = ['A', 'B', 'C']

    for file_name_end in file_list:
        input_file = f'../data/sample_fx_data_{file_name_end}.csv.gz'
        output_file = f'../output/minutely_data_{file_name_end}.csv.gz'

        print(input_file, output_file)

        data = load_data(input_file)
        validated_data = validate_data(data)
        cleaned_data = clean_data(validated_data)
        transformed_data = transform_data(cleaned_data)
        save_data(transformed_data, output_file)