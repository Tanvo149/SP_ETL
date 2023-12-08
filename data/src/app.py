from etl.extractor import read_json_to_df
from etl.transformer import (
    convert_dates,
    currency_filter,
    remove_duplicate_txnID,
    split_description,
    latest_customers_txn,
    final_df,
    add_additional_dates,
    data_quality_check,
)
from etl.loader import upsert_df_to_postgres
import os
import shutil

source_folder = "data/raw_files"
destination_folder = "data/successful"


def main():
    file_names = os.listdir(source_folder)

    for file_name in file_names:
        # Extract json file from source folder
        source_path = os.path.join(source_folder, file_name)
        df = read_json_to_df(source_path)

        # Transformation
        df, invalid_data_df = convert_dates(df)
        df, invalid_currency_df = currency_filter(df)
        df, duplicate_data_df = remove_duplicate_txnID(df)
        df = split_description(df)
        df = final_df(df)

        # Load errors to error_logs table and check for quality data
        """
        DQ for invalid txnDate
        """
        upsert_df_to_postgres(invalid_data_df, "error_logs")
        print(
            "{} records have been removed due to invalid transactionDate to the error logs table".format(
                len(invalid_data_df)
            )
        )
        data_quality_check(invalid_data_df, df, "invalid txnDate", 0.2)
        """
        DQ for invalid currency
        """
        upsert_df_to_postgres(invalid_currency_df, "error_logs")
        print("{} records have been removed due to invalid currency to the error logs".format(len(invalid_currency_df)))
        data_quality_check(invalid_currency_df, df, "invalid currencies", 0.2)
        """
        DQ for duplicate records
        """
        upsert_df_to_postgres(duplicate_data_df, "error_logs")
        print(
            "{} records have been removed due to duplcaite txnID to the error logs table".format(len(duplicate_data_df))
        )
        data_quality_check(duplicate_data_df, df, "invalid txnDate", 0.2)

        # Load latest transaction to customer table
        df_customers = latest_customers_txn(df)
        upsert_df_to_postgres(df_customers, "customers")

        # Load transaction records to transaction table
        df_transactions = add_additional_dates(df)
        upsert_df_to_postgres(df_transactions, "transactions")

        # Move file to successful folder after read the json file
        destination_path = os.path.join(destination_folder, file_name)
        shutil.move(source_path, destination_path)


if __name__ == "__main__":
    main()
