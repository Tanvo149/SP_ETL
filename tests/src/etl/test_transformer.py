import pytest
import pandas as pd
import sys
import os
from data.src.etl.transformer import convert_dates, currency_filter, remove_duplicate_txnID


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
        "customerID": ["b1", "b1", "b1"],
        "customerName": ["Bob", "Bob", "Bob"],
        "description": ["RyanAir | Travel", "RyanAir | Travel", "RyanAir | Travel"],
    }

    duplicate_df = pd.DataFrame(data)
    duplicate_df["sourceDate"] = pd.to_datetime(duplicate_df["sourceDate"])

    return duplicate_df


def test_remove_invalid_txnDate(sample_data):
    result_df = convert_dates(sample_data)

    assert len(result_df) == len(sample_data) - 1  # Delete invalid_date record


def test_sourceData_formatting(sample_data):
    result_df = convert_dates(sample_data).reset_index(drop=True)

    expected_data = {"sourceDate": ["2022-01-22 22:22:22", "2022-02-10 12:30:00", "2022-04-10 08:45:00"]}
    expected_df = pd.DataFrame(expected_data)
    expected_df["sourceDate"] = pd.to_datetime(expected_df["sourceDate"])

    assert result_df["sourceDate"].equals(expected_df["sourceDate"])


def test_currency_filter(sample_data):
    result_df = currency_filter(sample_data)

    assert len(result_df) == 3  # Valid currencies


def test_remove_duplicate_txnID(duplicate_data):
    result_df = remove_duplicate_txnID(duplicate_data).reset_index(drop=True)

    expected_data = {
        "transactionId": ["AB"],
        "sourceDate": ["2022-04-10 08:45:00"],
        "customerID": ["b1"],
        "customerName": ["Bob"],
        "description": ["RyanAir | Travel"],
    }

    expected_df = pd.DataFrame(expected_data)
    expected_df["sourceDate"] = pd.to_datetime(expected_df["sourceDate"])

    assert result_df.equals(expected_df)
