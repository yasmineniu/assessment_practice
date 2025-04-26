import pandas as pd
import re
import os
from datetime import datetime
from functools import wraps
import argparse
from logger_config import setup_logger
logger = setup_logger('util')

# to uniform the date column as YYYY-MM-DD, also handling invalid date
def clean_mixed_date_column(df, date_col='date'):
    cleaned_dates = []
    last_valid_date = None

    for i, raw_date in enumerate(df[date_col]):
        parsed_date = None

        # Try different formats: "%d %b %Y", "%d-%b-%Y"
        for fmt in ("%d %b %Y", "%d-%b-%Y"):
            try:
                parsed_date = datetime.strptime(str(raw_date), fmt)
                break
            except ValueError:
                continue

        # Handle invalid date ('40 Jan 2024')
        if not parsed_date:
            try:
                parsed_date = pd.to_datetime(raw_date, errors='raise')
            except Exception:
                logger.warning(f"found invalid date {parsed_date}, replace the value to the last valid date, ")
                parsed_date = last_valid_date  # fallback to previous

        cleaned_dates.append(parsed_date)
        last_valid_date = parsed_date

    # Set the cleaned column with desired format
    df[date_col] = [d.strftime("%d-%b-%Y") if pd.notnull(d) else np.nan for d in cleaned_dates]
    return df

def cleansed_merit_table(merit_file_path):
    try:
        if not os.path.exists(merit_file_path):
            logger.error(f"File not found: {merit_file_path}")
            return
    
        df_merit = pd.read_csv(merit_file_path, skiprows=2)
        # standardize column name
        df_merit.columns = [re.sub(r"\s*\(.*?\)", "", col).strip().lower() for col in df_merit.columns]
        # cleaning and parsing date column
        df_merit = clean_mixed_date_column(df_merit)
        # keep numeric format
        df_merit['lowest to highest offer price'] = pd.to_numeric(df_merit['lowest to highest offer price'], errors='coerce')
        df_merit['total offer capacity at specified offer price'] = pd.to_numeric(df_merit['total offer capacity at specified offer price'], errors='coerce')
        # Changing column name
        df_merit = df_merit.rename(columns = {'lowest to highest offer price': "bid_price"})
        df_merit = df_merit.rename(columns = {'total offer capacity at specified offer price': "bid_volumn"})
        logger.info("merit table columns cleaned and converted.")

        # Warnings: check if any value is null or duplicated row
        total_nulls = df_merit.isnull().sum().sum()
        if total_nulls > 0:
            msg = f"Detected {total_nulls} missing/null values in the merit dataframe."
            logger.error(msg)
            raise ValueError(msg)

        if df_merit.duplicated().any():
            logger.warning("Duplicated rows found in the merit dataframe.")

        return df_merit
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)} in the merit dataframe")
        return None

def cleansed_usep_table(usep_file_path):
    try:
        if not os.path.exists(usep_file_path):
            logger.error(f"File not found: {usep_file_path}")
            return

        df_usep = pd.read_csv(usep_file_path)

        df_usep.columns = [re.sub(r"\s*\(.*?\)", "", col).strip().lower() for col in df_usep.columns]
        df_usep = clean_mixed_date_column(df_usep)
        df_usep['demand'] = pd.to_numeric(df_usep['demand'], errors='coerce')
        df_usep['usep'] = pd.to_numeric(df_usep['usep'], errors='coerce')
        df_usep['lcp'] = pd.to_numeric(df_usep['lcp'], errors='coerce')
        df_usep['tcl'] = pd.to_numeric(df_usep['tcl'], errors='coerce')
        logger.info("user table columns cleaned and converted.")
        print(df_usep.head(5))

        # Warnings: check if any value is null or duplicated row
        total_nulls = df_usep.isnull().sum().sum()
        if total_nulls > 0:
            msg = f"Detected {total_nulls} missing/null values in the user dataframe."
            logger.error(msg)
            raise ValueError(msg)

        if df_usep.duplicated().any():
            logger.warning("Duplicated rows found in the user dataframe.")

        return df_usep
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}in the user dataframe.")
        return None 
    
def generate_datetime_index(df, date_col = 'date', period_col = 'period'):
    df[date_col] = pd.to_datetime(df[date_col], format='%d-%b-%Y')
    # based on the period; if period is 1 then time will be 00:30:00, 2 will be 1:00:00, so on so forth to create timestamp
    df["time"] = pd.to_timedelta((df[period_col] - 1) * 30, unit='min')
    df['datetime'] = df[date_col] + df['time']
    df = df.set_index('datetime')
    df = df.drop(columns=[date_col, period_col, 'time'])
    return df

# Select rows matching a specific date and time period
def select_certain_period(df, date_str, period, period_duration = '30min'):
    """
    Args:
        df: DataFrame with datetime index
        target_date: Date as string ('YYYY-MM-DD') or datetime object
        period: Integer representing period (1-48 for 30min)
        period_duration: Time interval ('30min')
    
    Returns:
        Filtered DataFrame
    """
    date = pd.to_datetime(date_str).normalize()
    print(date)

    if period_duration == '30min':
        time_offset = pd.to_timedelta((period-1)*30, unit='min')
        logger.info(time_offset)

    else:
        raise ValueError("period_duration must be '30min'")
    
    target_timestamp = date + time_offset
    print(target_timestamp)
    print(df)
    print(df.loc[target_timestamp])
    return df.loc[target_timestamp]


# calculate the cumulative bid volumn for ceretain peiord
def cumulative_vol_for_certain_period(df): 
    plot_merit = pd.DataFrame(df).sort_values('bid_price', ascending=True)

    logger.info("calculate the cumulative volumn")
    plot_merit['cumulative_volume'] = plot_merit['bid_volumn'].cumsum()
    max_volume = plot_merit['cumulative_volume'].max()
    return plot_merit, max_volume

# to validate the date input is follow 
def validate_date(date_str):
    """Validate that date is in YYYY-MM-DD format and between 2023-01-01 and 2023-01-31"""
    if not re.match(r'^2023-0[1-9]-[0-2][0-9]$|^2023-01-3[01]$', date_str):
        raise argparse.ArgumentTypeError(f"Date must be in exactly YYYY-MM-DD format with leading zeros (2023-01-01 to 2023-01-31)")
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Date {date_str} is not in YYYY-MM-DD format")
    
    min_date = datetime.strptime("2023-01-01", "%Y-%m-%d").date()
    max_date = datetime.strptime("2023-01-31", "%Y-%m-%d").date()
    
    if not (min_date <= date <= max_date):
        raise argparse.ArgumentTypeError(f"Date must be between 2023-01-01 and 2023-01-31")
    
    return date_str
