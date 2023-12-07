import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read("config.ini")


def write_to_csv(columns, csv_path):
    df = pd.DataFrame(cur.fetchall(), columns=columns)
    df.to_csv(csv_path, index=False)


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

    agg_by_customer = """ 
                        SELECT "customerId", currency, year, month, sum(amount) 
                        FROM transactions
                        GROUP BY "customerId", currency, year, month
                        ORDER BY "customerId", currency, year DESC, month DESC
                    """

    cur.execute(agg_by_customer)
    columns = [desc[0] for desc in cur.description]
    write_to_csv(columns, "SQL/outputs/customer_yymm_amount.csv")

    agg_by_category = """
                        SELECT category, currency, year, month, sum(amount)
                        FROM transactions
                        GROUP BY category, currency, year, month
                        ORDER BY category, currency, year DESC, month DESC
                    """

    cur.execute(agg_by_category)
    columns = [desc[0] for desc in cur.description]
    write_to_csv(columns, "SQL/outputs/category_yymmm_amount.csv")

    conn.commit()

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
