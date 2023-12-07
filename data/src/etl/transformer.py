import pandas as pd

pd.options.mode.copy_on_write = True


def convert_dates(df):
    """
    Transform the column transactionDate to date
    Transform the column sourceData format to yyyy-mm-dd HH:MM:SS
    Create error log report to Postgres if the column transactionDate is invalid
    """

    # set invalid date to NaT
    df["txn_date_1"] = pd.to_datetime(df["transactionDate"], errors="coerce")
    # seperate DF for invalid date to be process for error log
    invalid_date_df = df[df["txn_date_1"].isna()]

    if len(invalid_date_df) > 0:
        invalid_date_df["reason"] = "invalid transaction date format"
        invalid_date_df = invalid_date_df.drop(["customerName", "description", "txn_date_1"], axis=1)

    # Exclude rows with invalid date from the main dataframe
    df = df.dropna(subset=["txn_date_1"])
    df = df.drop("txn_date_1", axis=1)
    df["transactionDate"] = pd.to_datetime(df["transactionDate"])

    print("{} records have been removed for invalid transactionDate".format(len(invalid_date_df)))

    # Convert the sourceDate column to date format
    df["sourceDate"] = pd.to_datetime(df["sourceDate"])
    df.loc[:, "sourceDate"] = df["sourceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df, invalid_date_df


def currency_filter(df):
    # List of allowed currencies
    valid_list = ["EUR", "GBP", "USD"]

    # filter data for non-valid currencies to be progress to error log
    non_valid_list_df = df[~df["currency"].isin(valid_list)]
    print("{} records have been removed that were not in {}".format(len(non_valid_list_df), valid_list))

    if len(non_valid_list_df) > 0:
        non_valid_list_df["reason"] = "invalid currency"
        non_valid_list_df = non_valid_list_df.drop(["customerName", "description"], axis=1)

    return df[df["currency"].isin(valid_list)], non_valid_list_df


def remove_duplicate_txnID(df):
    """
    Remove duplicate reocrds by txnId and sourceDate
    """

    df = df.sort_values(by=["transactionId", "sourceDate"], ascending=[True, False])

    # Idenitfy duplicate reocrds including the latest transaction
    # Push to error log if any duplicate reocrds
    duplicate_df = df.duplicated(subset="transactionId", keep="first")
    if len(duplicate_df) > 0:
        duplicate_df = df[duplicate_df]
        duplicate_df["reason"] = "duplicate transactionId records"
        duplicate_df = duplicate_df.drop(["customerName", "description"], axis=1)

    # Remove duplicate txnIds and keep the first record
    df = df.drop_duplicates(subset="transactionId", keep="first")
    print("{} duplicate records have been removed".format(len(duplicate_df)))

    return df, duplicate_df


def add_additional_dates(df):
    df["year"] = df["transactionDate"].dt.year
    df["month"] = df["transactionDate"].dt.month
    df["day"] = df["transactionDate"].dt.day

    return df


def final_df(df):
    """
    Make final adjustment to dataframe before loading to Datamart
    """

    columns_to_drop = ["customerName", "description", "description_boolean"]
    df = df.drop(columns_to_drop, axis=1)

    df["amount"] = df["amount"].astype(float)

    return df


def split_description(df):
    """
    Split description column to merchant and category.
    Set free text to "Custom" as it may contain PPI
    """

    df["description_boolean"] = df["description"].str.contains("|", regex=False)
    split_columns = df["description"].str.split(r"\|", n=1, expand=True)
    split_columns = split_columns.rename(columns={0: "merchant", 1: "category"})

    df = pd.concat([df, split_columns], axis=1)

    df.loc[~df["description_boolean"], "merchant"] = "Custom Merchant"
    df.loc[~df["description_boolean"], "category"] = "Custom Category"

    df['merchant'] = df['merchant'].apply(check_phone_number)

    return df

def check_phone_number(value):
    """
    Description could have 075**** | Shopping 
    However, this could would need to be refine to identify phone number, 
    account number/sort code, email address. 
    WORK IN PROGRESS
    """
    if str(value).startswith('07'):
        return "Custom Merchant"
    else:
        return value

def latest_customers_txn(df):
    """
    If there are more than 1 latest txns on the same day,
    then use latest sourceDate to find the latest txn.
    """
    df = df.sort_values(by=["customerId", "transactionDate", "sourceDate"], ascending=[True, False, False])

    df = df.drop_duplicates(subset="customerId", keep="first")

    return df
