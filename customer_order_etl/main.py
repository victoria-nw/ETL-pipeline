import pandas as pd
import numpy
import logging
import sqlite3
import fastparquet


def setup_logging():
    """SETTING LOGGING HANDLERS"""
    file_handler = logging.FileHandler('test.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(filename)s:%(message)s'))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))

    logging.basicConfig(level = logging.INFO,
                        handlers=[file_handler, console_handler])

    logging.info('=' * 80)
    logging.info(' ')



def loading_data(filepath):
    """READING THE DATA"""
    try:
        df_raw = pd.read_csv(filepath)
        logging.info('raw data loaded successfully')
        print (df_raw.head())
        logging.info (f'Raw data shape: {df_raw.shape}')
    except FileNotFoundError:
        logging.error('Error: file not found. ensure file is in the right directory')
        exit()
    return df_raw



def validate_data(df):
    """VALIDATING THE DATA"""

    logging.info('Starting data validation')
    df_validated = df.copy() # this ensures we dont repeat data
    initial_rows = len(df_validated)

    # checking info
    df.info()
    df.describe()

    # Check for missing values 
    missing_values = df_validated.isnull().sum()
    if missing_values.any():
        logging.warning(f'Found missing values:')
        for col, count in missing_values[missing_values > 0].items():
            logging.warning(f'  {col}: {count} missing values')
        # Option: drop rows with any missing values
        df_validated = df_validated.dropna()
        logging.info(f'Dropped {initial_rows - len(df_validated)} rows with missing values')

    # Check for duplicate orders 
    duplicates = df_validated.duplicated(subset=['order_id'])
    if duplicates.any():
        duplicate_count = duplicates.sum()
        logging.warning(f'Found {duplicate_count} duplicate order_ids - removing duplicates')
        df_validated = df_validated.drop_duplicates(subset=['order_id'], keep='first')

    # negative quantities
    negative_quantity = df_validated[df_validated['quantity'] < 0]
    if len(negative_quantity) > 0:
        logging.warning(f'removing {len(negative_quantity)} rows that contain negative quantities')
        df_validated = df_validated[df_validated['quantity'] >= 0]

    #  negative prices
    negative_price = df_validated[df_validated['price_usd'] < 0]
    if len(negative_price) > 0:
        logging.warning(f'removing {len(negative_price)} rows that contain negative prices')
        df_validated = df_validated[df_validated['price_usd'] >= 0]

     # Data quality check
    valid_status = ['completed', 'pending', 'cancelled']
    invalid_status = ~df_validated['status'].str.lower().isin(valid_status)
    if invalid_status.any():
        invalid_count = invalid_status.sum()
        logging.warning(f'Found {invalid_count} rows with invalid status - removing')
        df_validated = df_validated[~invalid_status]

    # summary
    rows_removed = initial_rows - len(df_validated)
    if rows_removed > 0:
        logging.info(f'validation complete --> {rows_removed} rows removed ')
    else:
        logging.info(f'validation complete --> all data valid')


    return df_validated



def transform_data(df):
    """TRANSFORMING THE DATA"""
    df_transformed = df.copy()

    # calculating total_order_value for each order item
    df_transformed['total_order_value'] = df_transformed['quantity'] * df_transformed['price_usd']
    logging.info('total_order_value column added')

    # converting order_date to datetime 
    df_transformed['order_date'] = pd.to_datetime(df_transformed['order_date']) 
    logging.info('transformed order_date')

    # extracting order_month
    df_transformed['order_month'] = df_transformed['order_date'].dt.strftime('%Y-%m')
    logging.info('order_month column created')

    # ensuring quantity and prise_usd are numeric
    df_transformed['quantity'] = pd.to_numeric(df_transformed['quantity'], errors='coerce')
    df_transformed['price_usd'] = pd.to_numeric(df_transformed['price_usd'],errors='coerce')
    logging.info('ensured quantity and price_usd are numeric')

    # dropping rows with null quantity and prise_usd columns
    df_transformed.dropna(subset=['quantity', 'price_usd'], inplace=True)
    logging.info('dropped empty quantity and prise_usd columns')

    # viewing transformed data
    logging.info('transformed data')
    print (df_transformed.head())
    logging.info(f'Raw data shape: {df_transformed.shape}')

    return df_transformed



def schema_enforcement(df_transformed):
     """Define target schema"""
     logging.info('Preparing data for loading')
     
     customer_orders = [
        'order_id',
        'order_date', 
        'order_month',
        'customer_id',
        'product_id',
        'quantity',
        'price_usd',
        'total_order_value',
        'status',
        'delivery_address'
    ]
     
     df_final = df_transformed[customer_orders]
     logging.info(f'Schema enforced - {len(customer_orders)} columns in correct order')
     return df_final



def incremental_loading(df_final, last_load_date=None):
    """Load only new records since last run"""

    if last_load_date:
        logging.info(f'Incremental load: filtering records after {last_load_date}')
        df_incremental = df_final[df_final['order_date'] > last_load_date]
        logging.info(f'Found {len(df_incremental)} new records')
        return df_incremental
    else:
        logging.info('Full load: no previous load date provided')
        return df_final



def load_to_database(df_incremental, db_name='processed_orders.db', table_name='customer_orders'):
    """TRANSFORMING THE DATA"""
    logging.info('loading data into sqlite database')

    try:
        conn = sqlite3.connect(db_name)
        df_incremental.to_sql('customer_orders', conn, if_exists='replace', index=False)
        logging.info(f'data successfully loaded into database: {db_name}')

        # verifying that the data was loaded
        c = conn.cursor()
        c.execute (f'SELECT COUNT(*) FROM {table_name}')
        row_count = c.fetchone()[0]
        logging.info(f'Verification: {row_count} rows found in database')
        conn.close()

    except Exception as e:
        logging.error(f'Failed to load data to database: {e}')
        raise



def save_backup_csv(df, output_filename='processed_orders.csv'):
    """SAVING TO CSV AS BACKUP"""
    try:
        df.to_csv(output_filename, index=False) 
        logging.info(f"Backup CSV saved to {output_filename}")
        return output_filename
    except Exception as e:
        logging.error(f'Failed to save CSV: {e}')
        raise



def verify_csv(output_filename):
    """VERIFYING OUTPUT FILE"""
    logging.info(f"Verifying output file '{output_filename}'...")
    try:
        df_processed = pd.read_csv(output_filename)
        logging.info("Verification successful. data head:")
        print(df_processed.head())
        logging.info(f"Processed data shape: {df_processed.shape}")
    except FileNotFoundError:
        logging.error(f"Error: Could not read '{output_filename}'")



def main():
    """MAIN ETL PIPELINE"""
    logging.info('=' * 80)
    logging.info('=== ETL Pipeline Started ===')
    logging.info('=' * 80)
    
    try:
        df_raw = loading_data('dataset.csv')  
        
        df_validated = validate_data(df_raw)
        
        df_transformed = transform_data(df_validated)

        df_final = schema_enforcement(df_transformed)

        last_run = '2022-12-01'  
        
        df_incremental = incremental_loading(df_final, last_run)
        
        load_to_database(df_incremental)

        output_file = save_backup_csv(df_incremental, 'processed_orders.csv')

        # save to parquet file
        df_incremental.to_parquet('processed_orders.parquet', index=False)
        logging.info('Backup Parquet saved')
        
        verify_csv(output_file)

        logging.info('=== ETL Pipeline Completed Successfully ===')
        logging.info('=' * 80)
        logging.info(' ')
        
    except Exception as e:
        logging.critical(f'Pipeline failed: {e}')
        raise

if __name__ == '__main__':
    setup_logging()
    main()
    