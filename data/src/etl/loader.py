from sqlalchemy import create_engine
import psycopg2
import configparser
import getpass


if getpass.getuser() is not None:
    username = getpass.getuser()
else:
    username = "Unknown"

config = configparser.ConfigParser()
config.read("config.ini")


def upsert_df_to_postgres(df, table_name):
    conn = None
    cur = None

    """
        Parameters:
        - df: The dataframe to be upsertted
        - table_name: The name of the target table in the Postgre DB 
        """

    conn = psycopg2.connect(
        host=config.get("Postgres_Credentials", "hostname"),
        dbname=config.get("Postgres_Credentials", "database"),
        user=config.get("Postgres_Credentials", "username"),
        password=config.get("Postgres_Credentials", "password"),
        port=config.get("Postgres_Credentials", "port_id"),
    )

    engine = create_engine(
        "postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}".format(
            h=config.get("Postgres_Credentials", "hostname"),
            db=config.get("Postgres_Credentials", "database"),
            u=config.get("Postgres_Credentials", "username"),
            pw=config.get("Postgres_Credentials", "password"),
            p=config.get("Postgres_Credentials", "port_id"),
        )
    )

    cur = conn.cursor()

    try:
        if table_name == "customers":
            insert_count = 0
            update_count = 0

            for index, row in df.iterrows():
                customer_id = row["customerId"]
                transaction_id = row["transactionId"]
                transaction_date = row["transactionDate"]
                source_date = row["sourceDate"]
                merchant_id = (row["merchantId"],)
                category_id = (row["categoryId"],)
                currency = (row["currency"],)
                amount = (row["amount"],)
                merchant = (row["merchant"],)
                category = row["category"]

                """
                    Update customers latest transaction if the condition meet
                    """

                cur.execute(
                    'SELECT * FROM customers WHERE "customerId" = %s AND \
                                "transactionDate" < %s AND \
                                "sourceDate" < %s ',
                    (customer_id, transaction_date, source_date),
                )
                customer_exists = cur.fetchone()

                if customer_exists:
                    update_query = """
                            UPDATE customers 
                            SET "transactionId" = %s,
                                "transactionDate" = %s,
                                "sourceDate" = %s,
                                "merchantId" = %s,
                                "categoryId" = %s,
                                "currency" = %s,
                                "amount" = %s,
                                "merchant" = %s, 
                                "category" = %s,
                                "last_user_updated" = %s
                            WHERE "customerId" = %s
                        """
                    try:
                        cur.execute(
                            update_query,
                            (
                                transaction_id,
                                transaction_date,
                                source_date,
                                merchant_id,
                                category_id,
                                currency,
                                amount,
                                merchant,
                                category,
                                username,
                                customer_id,
                            ),
                        )
                        print("{} has been updated in customers table".format(customer_id))
                        update_count += 1

                    except Exception as e:
                        print(f"Error updating customers data: {e}")
                        conn.rollback()  # Rollback in case of an error

                else:
                    """
                    Insert new records if customerId does not exist
                    If it exist, the code will still execute.
                    """
                    insert_query = """
                            INSERT INTO customers ("customerId", "transactionId",
                                                    "transactionDate", "sourceDate",
                                                    "merchantId", "categoryId",
                                                    "currency", "amount",
                                                    "merchant", "category",
                                                    "last_user_updated")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    try:
                        cur.execute(
                            insert_query,
                            (
                                customer_id,
                                transaction_id,
                                transaction_date,
                                source_date,
                                merchant_id,
                                category_id,
                                currency,
                                amount,
                                merchant,
                                category,
                                username,
                            ),
                        )
                        insert_count += 1
                    except psycopg2.IntegrityError as integrity_error:
                        if "duplicate key value violates unique constraint" in str(integrity_error):
                            print(
                                f"""CustomerID: {customer_id} already exists in customer table
                                    and txnID {transaction_id} is not the latest"""
                            )
                    except Exception as e:
                        print(f"Error inserting into customers data: {e}")
                        conn.rollback()  # Rollback in case of an error

            conn.commit()
            print(f"Number of rows in {table_name} updated:", update_count)
            print(f"Number of rows in {table_name} inserted:", insert_count)

        elif table_name == "transactions":
            insert_count = 0
            update_count = 0

            for index, row in df.iterrows():
                customer_id = row["customerId"]
                transaction_id = row["transactionId"]
                transaction_date = row["transactionDate"]
                source_date = row["sourceDate"]
                merchant_id = (row["merchantId"],)
                category_id = (row["categoryId"],)
                currency = (row["currency"],)
                amount = (row["amount"],)
                merchant = (row["merchant"],)
                category = (row["category"],)
                year = (row["year"],)
                month = (row["month"],)
                day = row["day"]

                """
                    If there is duplicate txnId, 
                    then it would update if only the sourceDate is the latest
                    """

                cur.execute(
                    'SELECT * FROM transactions WHERE "customerId" = %s AND \
                                "transactionId" = %s AND \
                                "sourceDate" < %s ',
                    (customer_id, transaction_id, source_date),
                )
                transactions_exists = cur.fetchone()

                if transactions_exists:
                    update_query = """
                            UPDATE transactions 
                            SET "transactionDate" = %s,
                                "sourceDate" = %s,
                                "merchantId" = %s,
                                "categoryId" = %s,
                                "currency" = %s,
                                "amount" = %s,
                                "merchant" = %s, 
                                "category" = %s,
                                "year" = %s,
                                "month" = %s,
                                "day" = %s, 
                                "last_user_updated" = %s
                            WHERE "customerId" = %s
                                AND "transactionId" = %s
                        """
                    try:
                        cur.execute(
                            update_query,
                            (
                                transaction_date,
                                source_date,
                                merchant_id,
                                category_id,
                                currency,
                                amount,
                                merchant,
                                category,
                                year,
                                month,
                                day,
                                username,
                                customer_id,
                                transaction_id,
                            ),
                        )
                        update_count += 1
                    except Exception as e:
                        print(f"Error updating transactions data: {e}")
                        conn.rollback()  # Rollback in case of an error

                else:
                    insert_query = """
                            INSERT INTO transactions ("customerId", "transactionId",
                                                    "transactionDate", "sourceDate",
                                                    "merchantId", "categoryId",
                                                    "currency", "amount",
                                                    "merchant", "category",
                                                    "year", "month", "day",
                                                    "last_user_updated")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    try:
                        """
                        Insert record only if txn doesn't exist,
                        otherwise it won't insert
                        """
                        cur.execute(
                            insert_query,
                            (
                                customer_id,
                                transaction_id,
                                transaction_date,
                                source_date,
                                merchant_id,
                                category_id,
                                currency,
                                amount,
                                merchant,
                                category,
                                year,
                                month,
                                day,
                                username,
                            ),
                        )
                        insert_count += 1
                    except Exception as e:
                        print(f"Error inserting into transactions table: {e}")
                        conn.rollback()  # Rollback in case of an error

            conn.commit()
            print(f"Number of rows in {table_name} updated:", update_count)
            print(f"Number of rows in {table_name} inserted:", insert_count)

        elif table_name == "error_logs":
            try:
                # Write to error_logs table
                df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method="multi")
                conn.commit()
            except Exception as e:
                print(f"Error writing to error_logs table: {str(e)}")

        else:
            print("{} does not exist".format(table_name))

    except Exception as e:
        print(f"Error upserting DataFrame to PostgreSQL: {e}")

    finally:
        # Close the connection
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()
