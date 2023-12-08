import pytest
import pandas as pd
from data.src.etl.transformer import (
    convert_dates,
    currency_filter,
    remove_duplicate_txnID,
    split_description,
    check_phone_number,
    latest_customers_txn,
    data_quality_check,
)


@pytest.fixture
def sample_data():
    data = {
        "transactionDate": ["2022-01-01", "2022-02-15", "invalid_date", "2022-02-16"],
        "sourceDate": ["2022-01-22T22:22:22", "2022-02-10T12:30:00", "2022-03-01T09:45:00", "2022-04-10T08:45:00"],
        "currency": ["EUR", "GBP", "USD", "BTC"],
        "customerName": ["Bob", "Bob", "Bob", "Bob"],
        "description": ["RyanAir | Travel", "RyanAir | Travel", "RyanAir | Travel", "RyanAir | Travel"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def duplicate_data():
    data = {
        "transactionId": ["AB", "AB", "AB"],
        "sourceDate": ["2022-01-22 22:22:22", "2022-02-10 12:30:00", "2022-04-10 08:45:00"],
        "customerId": ["b1", "b1", "b1"],
        "customerName": ["Bob", "Bob", "Bob"],
        "description": ["RyanAir | Travel", "RyanAir | Travel", "RyanAir | Travel"],
    }

    duplicate_df = pd.DataFrame(data)
    duplicate_df["sourceDate"] = pd.to_datetime(duplicate_df["sourceDate"])

    return duplicate_df


@pytest.fixture
def latest_txn_data():
    data = {
        "customerId": ["a1", "b1", "a1", "b1"],
        "transactionDate": ["2022-01-01", "2022-02-01", "2022-03-01", "2022-04-01"],
        "sourceDate": ["2022-01-22 22:22:22", "2022-02-22 22:22:22", "2022-03-22 22:22:22", "2022-04-22 22:22:22"],
    }

    return pd.DataFrame(data)


def test_remove_invalid_txnDate(sample_data):
    """
    Test the function to remove invalid transaction date
    """
    result_df, invalid_date_df = convert_dates(sample_data)

    assert len(result_df) == len(sample_data) - 1  # Delete invalid_date record
    assert len(invalid_date_df) == 1  # Expect 1 record


def test_sourceDate_formatting(sample_data):
    """
    Test the function format sourceDate
    """
    result_df, invalid_date_df = convert_dates(sample_data)
    result_df = result_df.reset_index(drop=True)

    expected_data = {"sourceDate": ["2022-01-22 22:22:22", "2022-02-10 12:30:00", "2022-04-10 08:45:00"]}
    expected_df = pd.DataFrame(expected_data)
    expected_df["sourceDate"] = pd.to_datetime(expected_df["sourceDate"])

    assert result_df["sourceDate"].equals(expected_df["sourceDate"])


def test_currency_filter(sample_data):
    """
    Test the function to filter out invalid currency
    """
    result_df, invalid_currency_df = currency_filter(sample_data)

    assert len(result_df) == 3  # Valid currencies
    assert len(invalid_currency_df) == 1  # Invalid currency


def test_remove_duplicate_txnID(duplicate_data):
    """'
    Test the function to remove duplicate txn and keeping the first latest record
    """
    result_df, duplicate_df = remove_duplicate_txnID(duplicate_data)
    result_df = result_df.reset_index(drop=True)

    expected_data = {
        "transactionId": ["AB"],
        "sourceDate": ["2022-04-10 08:45:00"],
        "customerId": ["b1"],
        "customerName": ["Bob"],
        "description": ["RyanAir | Travel"],
    }

    expected_df = pd.DataFrame(expected_data)
    expected_df["sourceDate"] = pd.to_datetime(expected_df["sourceDate"])

    assert result_df.equals(expected_df)


def test_split_description(sample_data):
    """
    Test the function to split description into merchant and category
    """
    result_df = split_description(sample_data)

    expected_data = {
        "merchant": ["RyanAir ", "RyanAir ", "RyanAir ", "RyanAir "],
        "category": [" Travel", " Travel", " Travel", " Travel"],
    }
    expected_df = pd.DataFrame(expected_data)
    print(result_df["category"])
    print(expected_df["category"])

    assert result_df["merchant"].equals(expected_df["merchant"])
    assert result_df["category"].equals(expected_df["category"])


def test_check_phone_number_start_with_07():
    """
    Test the function to identify number start with 07
    However, this test is very weak. Need to imrpove
    """
    result = check_phone_number("072345694323")
    assert result == "Custom Merchant"


def test_check_phone_number_start_with_not_07():
    result = check_phone_number("0123")
    assert result == "0123"


def test_latest_customer_txn(latest_txn_data):
    """
    Test the function that the data is populating latest transaction per customer
    """
    result_df = latest_customers_txn(latest_txn_data)
    result_df = result_df.reset_index(drop=True)
    expected_data = {
        "customerId": ["a1", "b1"],
        "transactionDate": ["2022-03-01", "2022-04-01"],
        "sourceDate": ["2022-03-22 22:22:22", "2022-04-22 22:22:22"],
    }

    expected_df = pd.DataFrame(expected_data)
    assert result_df.equals(expected_df)


def test_data_quality_check_failed():
    """
    Test DA to raise exception if it breach the threshold
    """
    original_data = {"column1": [1, 2, 3, 4, 5], "column2": ["A", "B", "C", "D", "E"]}
    original_df = pd.DataFrame(original_data)

    error_data = {"column1": [1, 2], "column2": ["A", "B"]}
    data_error_df = pd.DataFrame(error_data)

    # raises exception
    with pytest.raises(Exception, match="Data Quality check failed breach 0.2% threshold: test"):
        data_quality_check(data_error_df, original_df, "test", 0.2)


def test_data_quality_check_passes():
    original_data = {"column1": [1, 2, 3, 4, 5], "column2": ["A", "B", "C", "D", "E"]}
    original_df = pd.DataFrame(original_data)

    original_data_2 = {"column1": [1], "column2": ["A"]}
    original_df_2 = pd.DataFrame(original_data_2)

    # raise no exception
    data_quality_check(original_df_2, original_df, "test", 0.5)
