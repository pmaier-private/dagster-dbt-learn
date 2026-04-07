# %%
"""Script to upload a CSV file as a table in PostgreSQL."""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys


# %%
db_host = "localhost"
db_user = "postgres"
db_password = "postgres"
db_name = "dagster"
table_name = "raw_table"
csv_file = "sample_data/sample_calls_25.csv"

# %%
df = pd.read_csv(csv_file)
print(f"✓ CSV file loaded: {csv_file} ({len(df)} rows)")

# %% [markdown]
# ## Cell 3: Write to Database

# %%
connection_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_string)
# %%    
df.to_sql(table_name, engine, if_exists='fail', index=False)
print(f"✓ Table '{table_name}' created/updated in database '{db_name}'")




# %%
