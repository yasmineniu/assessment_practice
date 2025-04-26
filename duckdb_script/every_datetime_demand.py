import duckdb
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger_config import setup_logger


logger = setup_logger('every_datetime_demand')
con = duckdb.connect("data.duckdb")

def fetch_table_as_df(table_name):
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()


# 5.d for every datetime and demand present in the demand file, calculate the final price

def every_datetime_demand():
    # sql script to join the merit_cumulative_volumn and usep table, to match the demand with cumulative bid volumn
    every_datetime_demand = """
    WITH joined_data AS (
    SELECT C.*, U.usep_price, U.demand_mw
    FROM merit_cumulative_volumn C LEFT JOIN user_validation U
    on C.date = U.date and C.period = U.period
    where C.cumulative_bid_volumn >= U.demand_mw
    ),
    ranked_data AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY date, period
        ORDER BY cumulative_bid_volumn ASC
    ) AS rn
    FROM joined_data
    )
    SELECT date, period, demand_mw, bid_price as final_bid_price, cumulative_bid_volumn
    FROM ranked_data
    WHERE rn = 1
    ORDER BY date, period
"""
    logger.info("ready to execute the sql")
    logger.info("final result will be : datetime, period, demand, final_bid_price, cumulative_bid_volumn")
    result_df = con.execute(every_datetime_demand).fetchdf()
    print(result_df.head())
    logger.info("save the result to every_datetime_demand.csv ")
    result_df.to_csv("every_datetime_demand.csv")

def main():
    every_datetime_demand()

if __name__ == "__main__":
    main()
    
