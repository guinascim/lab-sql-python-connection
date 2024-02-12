import pandas as pd
from sqlalchemy import create_engine
from getpass import getpass

username = "root"
password = getpass
database = "sakila"
engine = create_engine(f'mysql://{username}:{password}@localhost/{database_name}')


def rentals_month(engine, month, year):
    query = f"""
    SELECT customer_id, COUNT(rental_id) AS rentals_{month}_{year}
    FROM rental
    WHERE EXTRACT(MONTH FROM rental_date) = {month}
    AND EXTRACT(YEAR FROM rental_date) = {year}
    GROUP BY customer_id
    """
    df = pd.read_sql_query(query, engine)
    return df

def rental_count_month(df, month, year):
    new_column_name = f"rentals_{month}_{year}"
    df.rename(columns={new_column_name: 'rental_count'}, inplace=True)
    return df

def compare_rentals(df1, df2):
    merged_df = pd.merge(df1, df2, on='customer_id', how='outer')
    merged_df['difference'] = merged_df['rental_count_y'].sub(merged_df['rental_count_x'], fill_value=0)
    return merged_df


rentals_may_2005 = rentals_month(engine, 5, 2005)

rentals_june_2005 = rentals_month(engine, 6, 2005)

rentals_may_2005 = rental_count_month(rentals_may_2005, 5, 2005)
rentals_june_2005 = rental_count_month(rentals_june_2005, 6, 2005)

comparison_df = compare_rentals(rentals_may_2005, rentals_june_2005)

print(comparison_df)
