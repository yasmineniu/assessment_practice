from cleansed_file import cleansed_file
import matplotlib.pyplot as plt
import pandas as pd
from logger_config import setup_logger
import argparse
from util import select_certain_period, cumulative_vol_for_certain_period

# Initialize logger
logger = setup_logger('plot_merit_order')

# to plot merit table for certain date and period

def plot_merit_table(plot_merit):
    plt.figure(figsize=(10, 6))
    plt.step(plot_merit['cumulative_volume'], plot_merit['bid_price'], 
            where='post', linewidth=2, color='b')
    
    # formatting
    plt.title(f'Bid Stack Curve \n(Cumulative Volume vs. Bid Price)', pad=20)
    plt.xlabel('Cumulative Volume (MW)')
    plt.ylabel('Bid Price ($/MWh)')
    plt.grid(True, linestyle='--', alpha=0.7)

    # Annotate key points
    for _, row in plot_merit.iterrows():
        plt.annotate(f"{row['bid_price']} $", 
                    (row['cumulative_volume'], row['bid_price']),
                    textcoords="offset points",
                    xytext=(10,-5),
                    ha='left')


    plt.tight_layout()
    plt.savefig('bid_stack_curve.png', dpi=300, bbox_inches='tight')
    logger.info("the merit order for one day with the cumulative bid volume is saved")
    # Close the figure to free memory
    plt.close()


def main(args):
    merit_fulltable = pd.read_csv("cleansed_data/merit_cleansed.csv", index_col=0, parse_dates=True)
    logger.info("read cleansed merit table")
    selected_merit_table = select_certain_period(merit_fulltable, args.date, args.period)
    certain_time_merit_data, max_vol = cumulative_vol_for_certain_period(selected_merit_table)
    print(certain_time_merit_data)
    plot_merit_table(certain_time_merit_data)



if __name__ == "__main__":
    # 1st param is for date, 2nd param is for period to choose
    parser = argparse.ArgumentParser(description='input date and period data')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--period', type=int, choices=range(1, 49), 
                      help='30-min period (1-48)')
    # parser.add_argument('--output', default='result.csv', help='Output file path')
    args = parser.parse_args()
    main(args)
    

