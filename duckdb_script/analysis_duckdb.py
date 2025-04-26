import duckdb
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger_config import setup_logger


logger = setup_logger('duckdb_analysis')

con = duckdb.connect("data.duckdb")

# analysis - 1
# for each date, to find out the max/min power demand and the corresponding period
demand_limit_by_date = """
CREATE OR REPLACE TABLE demand_limit_by_date AS
WITH demand_extremes AS (
    SELECT
        date,
        MAX(demand_mw) AS max_demand,
        MIN(demand_mw) AS min_demand
    FROM
        user_validation
    GROUP BY
        date
)

SELECT
    t.date,
    t.period,
    t.demand_mw,
    CASE 
        WHEN t.demand_mw = de.max_demand THEN 'MAX'
        WHEN t.demand_mw = de.min_demand THEN 'MIN'
        ELSE NULL
    END AS demand_type
FROM
    user_validation t
JOIN
    demand_extremes de
ON
    t.date = de.date
WHERE
    t.demand_mw = de.max_demand OR t.demand_mw = de.min_demand
ORDER BY
    t.date, demand_type DESC;

"""
# demand_limit_by_date_df = con.execute(demand_limit_by_date).fetchdf()
# print(demand_limit_by_date_df)

#result: 
#          date  period  demand_mw demand_type
# 0  2023-01-01       9   5173.397         MIN
# 1  2023-01-01      41   6101.376         MAX
# 2  2023-01-02       9   5126.237         MIN
# 3  2023-01-02      40   6314.002         MAX
# 4  2023-01-03       9   5173.939         MIN
# ..        ...     ...        ...         ...
# 57 2023-01-29      42   6093.772         MAX
# 58 2023-01-30       8   5058.456         MIN
# 59 2023-01-30      34   6755.855         MAX
# 60 2023-01-31       8   5125.621         MIN
# 61 2023-01-31      31   6768.096         MAX

# analysis - 2, based on above result, To find which periods occur most frequently as the MAX demand type
period_for_MAX_demand = """
SELECT 
    period,
    COUNT(*) AS max_demand_count
FROM 
    demand_limit_by_date
WHERE 
    demand_type = 'MAX'
GROUP BY 
    period
ORDER BY 
    max_demand_count DESC
LIMIT 10;
"""
# period_for_MAX_demand_df = con.execute(period_for_MAX_demand).fetchdf()
# print(period_for_MAX_demand_df)

# period_for_MAX_demand_df:
#    period  max_demand_count
# 0      42                 4
# 1      40                 4
# 2      33                 3
# 3      34                 3
# 4      32                 3
# 5      41                 2
# 6      29                 2
# 7      28                 2
# 8      20                 2
# 9      31                 1
# for time period 28-34 and 40-42 will be the peak hour for demand




# analysis - 3, Time-of-Day Price Analysis
demand_fprice_by_date = """
SELECT 
    CASE 
        WHEN period BETWEEN 14 AND 36 THEN 'Daytime'
        WHEN period BETWEEN 37 AND 44 THEN 'Evening'
        WHEN period BETWEEN 45 AND 48 OR period BETWEEN 1 AND 13 THEN 'Night'
        ELSE 'Other'
    END AS time_of_day,
    AVG(usep_price) AS avg_price,
    AVG(demand_mw) AS avg_demand,
    COUNT(*) AS observation_count
FROM 
    user_validation
GROUP BY 
    time_of_day
ORDER BY 
    avg_price DESC;
"""
# demand_sum_by_date_df = con.execute(demand_fprice_by_date).fetchdf()
# print(demand_sum_by_date_df.head())

# result:
#   time_of_day   avg_price   avg_demand  observation_count
# 0     Evening  320.369355  6420.352948                248
# 1     Daytime  249.516034  6329.195218                711
# 2       Night  135.932277  5514.447226                527


# analysis - 4. Peak Demand Identification
peak_demand_identification = """
WITH ranked_demands AS (
    SELECT 
        date,
        period,
        demand_mw,
        usep_price,
        RANK() OVER (PARTITION BY date ORDER BY demand_mw DESC) AS demand_rank
    FROM 
        user_validation
)
SELECT 
    date,
    period,
    demand_mw,
    usep_price
FROM 
    ranked_demands
WHERE 
    demand_rank = 1
ORDER BY 
    demand_mw DESC;
"""
# peak_demand_identification_df = con.execute(peak_demand_identification).fetchdf()
# print(peak_demand_identification_df)

# result:
#          date  period  demand_mw  usep_price
# 0  2023-01-13      32   7044.935      392.93
# 1  2023-01-16      33   7021.481      336.68
# 2  2023-01-10      32   7019.439      333.10
# 3  2023-01-11      24   6960.853      308.77
# 4  2023-01-09      29   6925.352     1100.75

# analysis - 5. price volatility analysis
price_volatility_analysis = """
SELECT 
    date,
    STDDEV(usep_price) AS price_volatility,
    MAX(usep_price) - MIN(usep_price) AS price_range,
    (MAX(usep_price) - MIN(usep_price)) / AVG(usep_price) AS relative_volatility
FROM 
    user_validation
GROUP BY 
    date
ORDER BY 
    price_volatility DESC
LIMIT 10;
"""
# price_volatility_analysis = con.execute(price_volatility_analysis).fetchdf()
# print(price_volatility_analysis)
# result:
#         date  price_volatility  price_range  relative_volatility
# 0 2023-01-09       1070.927665      3854.67             3.751225
# 1 2023-01-17        764.840399      4394.59            12.594595
# 2 2023-01-26        157.058896       921.04             4.108962
# 3 2023-01-05        147.382946       451.64             1.612857
# 4 2023-01-08        139.021524       719.53             3.111941
# 5 2023-01-06        134.685992       683.44             2.501384
# 6 2023-01-16         91.279281       297.66             1.246571
# 7 2023-01-04         91.202852       255.57             1.084904
# 8 2023-01-13         90.323391       294.11             1.208263
# 9 2023-01-10         81.214693       383.99             1.376105
