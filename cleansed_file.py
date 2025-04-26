import pandas as pd
import re
import logging
from logger_config import setup_logger
from util import clean_mixed_date_column,generate_datetime_index,cleansed_usep_table,cleansed_merit_table
import os
import numpy
import util
from datetime import datetime
from functools import wraps
import sys
# adds the current working directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Initialize logger
logger = setup_logger('clean_file')

print("Current working directory:", os.getcwd())
print("Files in ../raw_data/:", os.listdir("raw_data/"))

def cleansed_file():
    merit_file_path = "raw_data/DelayedOfferStacks_Energy_01-Jan-2023 to 31-Jan-2023.csv"
    usep_file_path = "raw_data/USEP_Jan-2023.csv"
    df_usep = util.cleansed_usep_table(usep_file_path)
    df_merit = util.cleansed_merit_table(merit_file_path)
    logger.info("user table and merit table columns cleaned and converted.")

    df_usep_with_index = util.generate_datetime_index(df_usep)
    df_merit_with_index = util.generate_datetime_index(df_merit)
    logger.info("created index for user table and merit table.")
    merit_file_output_path = "cleansed_data/merit_cleansed.csv"
    usep_file_output_path = "cleansed_data/user_cleansed.csv"
    logger.info("write merit and user table to /cleansed_data folder.")
    df_usep_with_index.to_csv(usep_file_output_path)
    df_merit_with_index.to_csv(merit_file_output_path)

if __name__ == "__main__":
    cleansed_file()
