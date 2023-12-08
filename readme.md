# Tan's Project


## Step process to run the application 

Follow the step process below to set up your python application 

- To set up your PostgreSQL credentials, locate the `config.ini` file in the project directory and replace the placeholder and save the changes.  
- To create the necessary tables, run the following command `make create_tables`. This command will create the database tables for customers, transactions, error_logs and category.
- run `make setup` to set up the virtual environment and install the requirements. Make sure you activate the  virutal enviornment afterward. 
- check if there are any json data in the `data/raw_files/` before running the applications
- This command `make run` would run the main ETL application to the Postgre Tables. Once completed, the raw file will move to `successful` folder. The main application can be found here `data/src/app.py`.
- To test the UPSERT functionality for new file, the file can be found in `data/additional_json_files`. You will need to move the file to `data/raw_files` and run the command `make run` again. This would update customers table latest transaction and insert new records for new customers. 
- After you done with the project, you can run `make delete_tables` to drop all the tables from the database. ***Hooray!***

**If you get `ModuleNotFoundError` run `pip install .`**
**use the `requirements_v2.txt` to install the packages, however it should be included if you run `make setup`.

---
## Data Quality

The data quality check examines original data for duplicate transactions, invalid currencies, or invalid transaction dates, raising an exception if at least 20% of the data exhibits any of these issues.

---
## Postgres Tables 

#### Customer Table 
This shows a list of unique customers with the most recent transactions. 

| Column Name | Data Type | Description|
| --- | --- | --- |
| customerId| varchar(100)|primary key, customer identify|
| transactionId| varchar(100) | Unique transaction ID|
| transactionDate| DATE| Date of transaction|
| sourceDate | TIMESTAMP| The timestamp of the record was created in the source system|
| merchantId | INT|Merchant Identifier|
| categoryId | INT|Category Identifier|
| currency | FLOAT|Currency Code|
| amount | varchar(100)|Amount of transaction|
| merchant | varchar(100)|Merchant Name|
| category | varchar(30)|Category Name| 
|last_user_updated| varchar(100)| updated by user|
| last_updated| TIMESTAMP|When was the row last updated|

#### Transaction Table 
Shows the list of transactions 
| Column Name | Data Type | Description|
| --- | --- | --- |
| customerId| varchar(100)|primary key, customer identify|
| transactionId| DATE |primary key, Unique transaction ID|
| transactionDate| DATE| Date of transaction|
| sourceDate | TIMESTAMP| The timestamp of the record was created in the source system|
| merchantId | INT|Merchant Identifier|
| categoryId | INT|Category Identifier|
| currency | FLOAT|Currency Code|
| amount | varchar(100)|Amount of transaction|
| merchant | varchar(100)|Merchant Name|
| category | varchar(30)|Category Name| 
| year| INT|year of transaction|
| month| INT|month of transaction|
|day| INT| day of transaction|
|last_user_updated| varchar(100)| updated by user|
| last_updated| TIMESTAMP|When was the row last updated|

#### Error logs
Shows list of transactions that were corrupted. 
No primary key were set as there may be duplicate data. 

| Column Name | Data Type | Description|
| --- | --- | --- |
| customerId| varchar(100)|customer identify|
| transactionId| DATE |Unique transaction ID|
| transactionDate| TEXT| Date of transaction|
| sourceDate | varchar(100)| The timestamp of the record was created in the source system|
| merchantId | varchar(100)|Merchant Identifier|
| categoryId | varchar(100)|Category Identifier|
| currency | TEXT|Currency Code|
| amount | varchar(100)|Amount of transaction|
| reason | varchar(100)|reason for error|
| last_updated| TIMESTAMP|When was the row last updated|

#### Category 
This shows the category mapping. 
This table is not used, but can be used as a dimension table.
| Column Name | Data Type | Description|
| --- | --- | --- |
| categoryId| varchar(100)|category identify|
| category| DATE |category name|

---
## Additional Insight 

- The command `make reports` generate extracts from the database. The csv output can be found in `SQL/outputs` folder. 
- The file named `category_yymmm_amount.csv` provides insights into expenditures and refunds over time by aggregating amounts based on their categories, year, and month. The dataset contains both positive amounts (representing expenditures) and negative amounts (representing refunds). This categorisation allows for a comprehensive understanding of spending and refund patterns across different categories throughout the specified period.
- The file named `customer_yyyymm_amount.csv` consolidates the overall spending amounts for customers over the specified period. This dataset is designed to facilitate trend analysis, enabling a comprehensive view of customer spending patterns. 