import pandas as pd


def load_data(file_path):
    return pd.read_csv(file_path, compression='gzip')


def validate_data(df):
    # Add validation logic here
    return df.dropna()


def clean_data(df):
    # Add cleaning logic here
    return df


def transform_data(df):
    df['mid_price'] = (df['bid'] + df['ask']) / 2
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    # Resample to minute intervals
    minutely_data = df.resample('T').agg({
        'mid_price': ['first', 'max', 'min', 'last'],
        'volume': 'sum'
    })

    minutely_data.columns = ['open', 'high', 'low', 'close', 'volume']
    minutely_data.reset_index(inplace=True)

    return minutely_data


def save_data(df, output_path):
    df.to_csv(output_path, index=False, compression='gzip')


if __name__ == "__main__":
    input_file = '../data/sample_fx_data_C.csv.gz'
    output_file = '../output/minutely_data.csv.gz'

    data = load_data(input_file)
    validated_data = validate_data(data)
    cleaned_data = clean_data(validated_data)
    transformed_data = transform_data(cleaned_data)
    save_data(transformed_data, output_file)
