import duckdb
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger_config import setup_logger


logger = setup_logger('duckdb_cleansed_file')

con = duckdb.connect("data.duckdb")
def fetch_table_as_df(table_name):
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()

# 5.b perform cleaning to the raw table, this will generated 2 validated table
def cleansing_tables() -> None:
    cleaning_merit_sql = """
    -- Step 1: First identify valid dates and mark rows
    CREATE OR REPLACE TABLE merit_validation AS
    with validated_merit_data AS (
        SELECT
            Date as date,
            CASE WHEN "PERIOD" BETWEEN 1 AND 48 THEN "PERIOD" ELSE NULL END AS period,
            CASE WHEN "Total Offer Capacity At Specified Offer Price (MW)" >= 0 THEN "Total Offer Capacity At Specified Offer Price (MW)" ELSE NULL END AS bid_volumn,
            CASE WHEN TRY_CAST("Lowest to Highest Offer Price ($/MWh)" AS DOUBLE) IS NOT NULL THEN "Lowest to Highest Offer Price ($/MWh)" ELSE NULL END AS bid_price
        FROM RAW_MERIT_TABLE
    ),
    with_datetime_merit_data AS (
        SELECT date, period, bid_price, bid_volumn
        FROM validated_merit_data
        where date IS NOT NULL AND period IS NOT NULL
    )
    select *
    from with_datetime_merit_data
    """
    cleaning_user_sql = """
    -- Step 1: First identify valid dates and mark rows
    CREATE OR REPLACE TABLE user_validation AS
    with validated_user_data AS (
        SELECT
            "INFORMATION TYPE" AS info_type,
            CASE WHEN "PERIOD" BETWEEN 1 AND 48 THEN "PERIOD" ELSE NULL END AS period,
            CASE WHEN "USEP ($/MWh)" >= 0 THEN "USEP ($/MWh)" ELSE NULL END AS usep_price,
            CASE WHEN "LCP ($/MWh)" >= 0 THEN "LCP ($/MWh)" ELSE NULL END AS lcp_price,
            CASE WHEN "DEMAND (MW)" > 0 THEN "DEMAND (MW)" ELSE NULL END AS demand_mw,
            CASE WHEN "TCL (MW)" >= 0 THEN "TCL (MW)" ELSE NULL END AS tcl_mw,
            CASE 
                WHEN TRY_STRPTIME("DATE", '%d %b %Y') IS NOT NULL 
                AND DAY(STRPTIME("DATE", '%d %b %Y')) <= 31 
                THEN STRPTIME("DATE", '%d %b %Y')
                ELSE NULL
            END AS date
        FROM RAW_USER_TABLE
    ),
    with_datetime_user_data AS (
        select info_type, date, period, usep_price, lcp_price, demand_mw, tcl_mw,
        from validated_user_data
        where date IS NOT NULL AND period IS NOT NULL
    )

    select *
    from with_datetime_user_data
    """
    logger.info("ready to execute cleaning sql script")
    con.execute(cleaning_user_sql)
    con.execute(cleaning_merit_sql)

    # Get cleaned data
    cleaned_user_df = fetch_table_as_df("user_validation")
    logger.info("Cleaned User Data Sample:")
    logger.info(cleaned_user_df.head())
    cleaned_merit_df = fetch_table_as_df("merit_validation")
    logger.info("Cleaned Merit Data Sample:")
    logger.info(cleaned_merit_df.head())

def main():
    cleansing_tables()

if __name__ == "__main__":
    main()
    
