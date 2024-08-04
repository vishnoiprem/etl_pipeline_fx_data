# ETL Pipeline for FX Data

## Project Structure



# Data ETL Process

## Requirements

- Python 3.x
- pandas

## Instructions

1. Install the required packages:
    ```
    pip install pandas
    ```

2. Run the ETL script:
    ```
    python etl.py
    ```

3. The output file will be saved as `transformed_data.csv.gz`.

## Assumptions and Limitations

- The script assumes the input file is named `sample_fx_data_B.csv.gz` and is located in the same directory as the script.
- Only data for `USD/JPY` and `EUR/JPY` currency pairs from the year 2024 onwards are processed.
- The output is saved in a compressed CSV format.

## Optional Unit Tests

To run the unit tests, use the following command:
    ```
    python -m unittest discover tests
    ```
