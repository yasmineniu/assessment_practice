import pandas as pd
from logger_config import setup_logger
import argparse
from util import select_certain_period, cumulative_vol_for_certain_period, validate_date

# Initialize logger
logger = setup_logger('check_final_price')

# to plot merit table for certain date and period, and 
# def cumulative_vol_for_certain_period(df, date_str, period): 


def non_negative_demand_number(value:float):
    try:
        fvalue = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not a valid number")
    if fvalue < 0:
        raise argparse.ArgumentTypeError(f"{value} is not a non-negative number")
        
    return fvalue

def check_final_price(df, demand: float, max_vol: float):
    if demand > max_vol:
        raise ValueError(f"Demand {demand} exceeds maximum capacity {max_vol}")
    try:
        # Get the first row where cumulative volume meets or exceeds demand
        matched_rows = df[df['cumulative_volume'] >= demand]
        if len(matched_rows) == 0:
            # This should theoretically never happen if max_vol is correct
            raise ValueError(f"No bids available to meet demand {demand}")
            
        return matched_rows.iloc[0]['bid_price']
    except Exception as e:
        raise ValueError(f"Error finding clearing price: {str(e)}")



def main(args):
    merit_fulltable = pd.read_csv("merit_cleansed.csv", index_col=0, parse_dates=True)
    logger.info("read cleansed user table")
    # only select the certain date and period merit data
    selected_merit_table = select_certain_period(merit_fulltable, args.date, args.period)
    # new column to count the cumulative merit volumn, and get the max capacity
    certain_time_merit_data, max_vol = cumulative_vol_for_certain_period(selected_merit_table)
    final_price = check_final_price(certain_time_merit_data, args.demand, max_vol)
    print(f"based on the demand {args.demand}, final clearing price is {final_price}")



if __name__ == "__main__":
    # 1st param is for date, 2nd param is for period to choose
    parser = argparse.ArgumentParser(description='input date, period data and demand')
    parser.add_argument('--date', required=True, type=validate_date, help='Target date (YYYY-MM-DD)')
    #ensure period only be integar beween 1-48
    parser.add_argument('--period', type=int, choices=range(1, 49), 
                      help='30-min period (1-48)')
    #ensure demand only be the non-negative num
    parser.add_argument('--demand', required=True, type=non_negative_demand_number, help='Non-negative demand value (can be integer or float)')
    # parser.add_argument('--output', default='result.csv', help='Output file path')
    args = parser.parse_args()
    main(args)
    

