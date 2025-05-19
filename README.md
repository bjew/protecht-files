
# Project Documentation

## PART 1: Data Ingestion

The code for data ingestion is contained in the `main.py` file.  
The `requirements.txt` file specifies the required dependencies for the Python environment in which to run `main.py`.

The function uses the `KaggleDatasetAdapter` to download the specified dataset into a pandas DataFrame.

Then, a PostgreSQL connection is created using SQLAlchemy, and pandas `.to_sql()` is used to write the dataset to a table `forex` in the AWS PostgreSQL database:

```
currency-database-1.c0zuu4imcbei.us-east-1.rds.amazonaws.com
```

---

## PART 2: Data Modeling

Created the desired tables using the following PostgreSQL queries based on the existing `forex` table:

```sql
CREATE TABLE currency_metadata AS (
  SELECT 
    currency::text AS currency_symbol,
    currency_name::text AS currency_name
  FROM forex
);

CREATE TABLE exchange_rates AS (
  SELECT
    currency::text AS currency_symbol,
    date::date AS rate_date,
    exchange_rate::numeric AS exchange_rate
  FROM forex
);
```

---

## PART 3: Analysis

### Question 1: Currency Momentum Metrics

The query for question 1 is in the `kaggle.sql` file.

- A CTE is created that, for each currency/date combination, looks at the previous and current date’s `exchange_rate`, determines if there has been an increase, and populates a boolean column `increased`.
- Another CTE applies rules to determine if a date is part of a streak:
  1. It has to be an increase over the previous day,
  2. and the previous two days were also increases,
  3. OR the previous day and the next day were increases,
  4. OR the next two days were increases.
- The next CTE groups streak days into group IDs.
- Finally, it groups by currency and group ID to calculate average streak lengths and average percentage change over streaks.
- It then uses `RANK() OVER` to compute rankings and produce the final result set.

### Question 2: Custom Metrics

Two custom volatility metrics were analyzed:

- **`max_monthly_variance`**: The maximum percent difference between the highest and lowest exchange rates for a currency within a particular month.
- **`high_months_rank`**: The number of months where the variance was greater than 10%.

The SQL for this is also in `kaggle.sql`. It:

- Groups rows by year-month.
- Finds the `MIN` and `MAX` values for `exchange_rate`.
- Uses those to calculate the variance measures.

---

## BONUS

The SQL for this is also in `kaggle.sql`.

- Modified the query from Question 1 to just show `pct_change` and ranked the top 10.
- Added conditions to filter out `rate_date = CURRENT_DATE`.
- Re-ran the same CTEs to get the previous day's ranking.

The Python code to connect to PostgreSQL, run the query, and write to S3 is in the file `daily_export.py`.

---
