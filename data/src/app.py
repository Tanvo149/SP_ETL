from etl.extractor import read_json_to_df
from etl.transformer import (
    convert_dates,
    currency_filter,
    remove_duplicate_txnID,
    split_description,
    latest_customers_txn,
    final_df,
    add_additional_dates,
)
from etl.loader import upsert_df_to_postgres
import os
import shutil

source_folder = "data/raw_files"
destination_folder = "data/successful"


def main():
    file_names = os.listdir(source_folder)

    for file_name in file_names:
        # Extract from json
        # df = read_json_to_df('tech_assessment_transactions.json')
        source_path = os.path.join(source_folder, file_name)
        df = read_json_to_df(source_path)

        # Transformation
        df = convert_dates(df)
        df = currency_filter(df)
        df = remove_duplicate_txnID(df)
        df = split_description(df)
        df = final_df(df)

        # Load data to customers and transactions data mart
        df_customers = latest_customers_txn(df)
        upsert_df_to_postgres(df_customers, "customers")

        df_transactions = add_additional_dates(df)
        upsert_df_to_postgres(df_transactions, 'transactions')

        # Move file to successful folder after read the json file
        destination_path = os.path.join(destination_folder, file_name)
        shutil.move(source_path, destination_path)


if __name__ == "__main__":
    main()
