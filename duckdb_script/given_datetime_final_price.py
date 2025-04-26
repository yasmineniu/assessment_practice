import duckdb
import sys
import os
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger_config import setup_logger
from check_final_price import non_negative_demand_number
from util import validate_date

logger = setup_logger('given_datetime_final_price')
con = duckdb.connect("data.duckdb")

def fetch_table_as_df(table_name):
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()

# 5.c for a manually specified datetime and demand, calculate the final price
def given_datetime_final_price(date_str: str, period: int, demand:float):
    logger.info(f"param: date: {date_str}, period:{period}, demand:{demand}")
    given_param = (date_str, period, demand)
    # sql script to calculate cumulative volumn
    create_temp_sql = """
    CREATE OR REPLACE TABLE merit_cumulative_volumn AS
    WITH merit_cumulative AS (
        SELECT 
            *,
            SUM(bid_volumn) OVER (
                PARTITION BY date, period
                ORDER BY bid_price
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_bid_volumn
        FROM merit_validation
    )
    SELECT * FROM merit_cumulative;
    """
    logger.info("ready to execute sql script to calculate cumulative volumn")
    con.execute(create_temp_sql)

    # sql script to find the final price on certain datetime
    final_price_sql = """
    SELECT bid_price AS final_bid_price
    FROM merit_cumulative_volumn
    WHERE date = ? AND period = ? AND cumulative_bid_volumn >= ?
    ORDER BY bid_price ASC
    LIMIT 1
    """
    logger.info("ready to execute sql script to calculate final_bid_price")
    result_df = con.execute(final_price_sql, given_param).fetchdf()

    # get the final price
    if not result_df.empty:
        price = result_df.iloc[0]["final_bid_price"]
        print(f"The final clearing price for demand {demand} on {date_str} period {period} is: ${price:.2f}/MWh")
        logger.info(f"The final clearing price for demand {demand} on {date_str} period {period} is: ${price:.2f}/MWh")
    # if result_df is empty raise ValueError
    else:
        raise ValueError(f"No valid price found for demand {demand} on {date_str} period {period}.")


def main(args):
    given_datetime_final_price(args.date, args.period, args.demand)



if __name__ == "__main__":
    # 1st param is only for valid date, 2nd param is for period to choose between 1-28
    # 3rd param is a non negative float
    parser = argparse.ArgumentParser(description='input date, period and demand')
    parser.add_argument('--date', required=True, type=validate_date, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--period', type=int, choices=range(1, 49), 
                      help='30-min period (1-48)')
    parser.add_argument('--demand', type=non_negative_demand_number, help='Non-negative demand value (can be integer or float)')
    args = parser.parse_args()
    main(args)