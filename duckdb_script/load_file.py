import duckdb
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger_config import setup_logger


logger = setup_logger('load_duckdb_file')
con = duckdb.connect("data.duckdb")

def fetch_table_as_df(table_name):
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()

# 5.a load the raw merit order and demand file into tables
def load_file_into_db() -> None:
    merit_file_path = r"../raw_data/DelayedOfferStacks_Energy_01-Jan-2023 to 31-Jan-2023.csv"
    user_file_path = r"../raw_data/USEP_Jan-2023.csv"
    # create/replace table RAW_MERIT_TABLE, pre-define the column data type
    con.execute("""
                CREATE OR REPLACE TABLE RAW_MERIT_TABLE AS 
                SELECT * FROM 
                read_csv(?,
                skip = 2,
                header = true,
                delim = ',',
                columns = {
                    'Date': 'DATE',
                    'Period': 'INTEGER',
                    'Lowest to Highest Offer Price ($/MWh)': 'DOUBLE',
                    'Total Offer Capacity At Specified Offer Price (MW)': 'DOUBLE'
                },
                dateformat = '%d-%b-%Y'
                )
                """, [merit_file_path])
    # create/replace table RAW_USER_TABLE, pre-define the column data type
    # since user table DATE column 01 Jan 2023 cant directly change to date type, will put as varchar first
    con.execute("""
                CREATE OR REPLACE TABLE RAW_USER_TABLE AS 
                SELECT * FROM 
                read_csv(?,
                header = true,
                delim = ',',
                columns = {
                    'INFORMATION TYPE': 'VARCHAR',
                    'DATE': 'VARCHAR',
                    'PERIOD': 'INTEGER',
                    'USEP ($/MWh)': 'DOUBLE',
                    'LCP ($/MWh)': 'DOUBLE',
                    'DEMAND (MW)': 'DOUBLE',
                    'TCL (MW)': 'DOUBLE'
                })
                """, [user_file_path])

def main():
    load_file_into_db()

if __name__ == "__main__":
    main()
    
