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

    create_error_logs = """ CREATE TABLE IF NOT EXISTS error_logs (
                            "transactionId" varchar(100),
                            "customerId" varchar(100),
                            "transactionDate" TEXT,
                            "sourceDate" varchar(100),
                            "merchantId" varchar(100),
                            "categoryId" varchar(100), 
                            "currency" TEXT,
                            "amount" varchar(100),
                            "reason" varchar(100),
                            "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                    """

    cur.execute(create_error_logs)
    print("error_logs table created")

    create_customers = """ CREATE TABLE IF NOT EXISTS customers (
                            "customerId" varchar(100) PRIMARY KEY,
                            "transactionId" varchar(100),
                            "transactionDate" DATE,
                            "sourceDate" TIMESTAMP,
                            "merchantId" INT,
                            "categoryId" INT, 
                            "currency" varchar(100),
                            "amount" FLOAT,
                            "merchant" varchar(100),
                            "category" varchar(30),
                            "last_user_updated" varchar(100),
                            "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """
    cur.execute(create_customers)
    print("customer table created")

    create_transactions = """ CREATE TABLE IF NOT EXISTS transactions (
                            "customerId" varchar(100),
                            "transactionId" varchar(100),
                            "transactionDate" DATE,
                            "sourceDate" TIMESTAMP,
                            "merchantId" INT,
                            "categoryId" INT, 
                            "currency" VARCHAR(10),
                            "amount" FLOAT,
                            "merchant" varchar(100),
                            "category" varchar(30),
                            "year" INT,
                            "month" INT,
                            "day" INT,
                            "last_user_updated" varchar(100),
                            "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY ("customerId", "transactionId")
                            )
                        """
    cur.execute(create_transactions)
    print("transaction table created")

    create_category = """
                        CREATE TABLE IF NOT EXISTS category (
                            "categoryId" INT PRIMARY KEY, 
                            "category" varchar(30)
                        )
                        """
    cur.execute(create_category)
    print("category table created")

    insert_category = """INSERT INTO category ("categoryId", "category") VALUES (%s, %s)"""
    insert_values = [
        (1, "Transport"),
        (2, "Groceries"),
        (3, "Shopping"),
        (4, "Eating Out"),
        (5, "Entertainment"),
        (6, "Home & Family"),
        (7, "Charity"),
        (8, "Custom Category"),
        (9, "Heath & Beauty"),
        (10, "Travel"),
        (11, "General"),
    ]
    for record in insert_values:
        cur.execute(insert_category, record)

    conn.commit()

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
