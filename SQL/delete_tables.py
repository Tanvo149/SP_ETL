import psycopg2
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

conn = None
cur = None
try:
    conn = psycopg2.connect(
        host=config.get("Postgres_Credentials", "hostname"),
        dbname=config.get("Postgres_Credentials", "database"),
        user=config.get("Postgres_Credentials", "username"),
        password=config.get("Postgres_Credentials", "password"),
        port=config.get("Postgres_Credentials", "port_id"),
    )

    cur = conn.cursor()

    drop_error_logs = """     
                       DROP TABLE error_logs
                    """

    cur.execute(drop_error_logs)
    print("drop error_logs table")

    drop_customers = """ 
                        DROP TABLE customers
                        """
    cur.execute(drop_customers)
    print("drop customer table")

    drop_transactions = """ 
                        DROP TABLE transactions
                        """
    cur.execute(drop_transactions)
    print("drop transactions table")

    drop_category = """
                    DROP TABLE category
                    """
    cur.execute(drop_category)
    print("drop category table")

    conn.commit()

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
