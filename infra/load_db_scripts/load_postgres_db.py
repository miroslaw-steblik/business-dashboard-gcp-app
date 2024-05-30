""" Script to load the CSV data into the PostgreSQL database. """

import psycopg2
import os


# Connect to the PostgreSQL database server
conn = psycopg2.connect(
    host=os.environ["DB__HOST"],
    database=os.environ["DB__DBNAME"],
    user=os.environ["DB__USER"],
    password=os.environ["DB__PASSWORD"],
    port=os.environ["DB__PORT"]
)

# Create a new cursor
cur = conn.cursor()

# A simple query to create a table
cur.execute("DROP TABLE IF EXISTS client_main CASCADE;")
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS client_main (
        internal_id VARCHAR PRIMARY KEY,
        company_name VARCHAR(255),
        product_type VARCHAR(255),
        status VARCHAR(255),
        client_representative VARCHAR(255),
        contact_name VARCHAR(255),
        business_type VARCHAR(255),
        website VARCHAR(255),
        description TEXT, 
        city VARCHAR(255),
        email VARCHAR(255),
        onboarding_date DATE,
        created_at DATE
    )
    """
)

conn.commit() 

cur.execute("TRUNCATE client_main;") # Truncate the table before loading the data

with open("./data/client_database.csv", "r") as f:
    next(f)             # Skip the header row.
    cur.copy_expert("COPY client_main FROM STDIN CSV", f)
conn.commit()
print("Data loaded into client_main table")



# aum table
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS client_aum (
        UUID SERIAL PRIMARY KEY,
        internal_id VARCHAR REFERENCES client_main( internal_id),
        aum_date DATE,
        aum NUMERIC,
        contributions NUMERIC,
        active_members NUMERIC,
        deferred_members NUMERIC,
        created_at DATE
    )
    """
)

conn.commit()

cur.execute("TRUNCATE client_aum;") # Truncate the table before loading the data

for file in os.listdir("./data/aum"):
    with open(f"./data/aum/{file}", "r") as f:
        next(f)             # Skip the header row.    
        cur.copy_expert("COPY client_aum( \
                        internal_id, \
                        aum_date, \
                        aum, \
                        contributions, \
                        active_members, \
                        deferred_members, \
                        created_at) \
                        FROM STDIN CSV", f)
    conn.commit()
    print(f"{file} loaded")

# fee table
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS client_fee (
        UUID SERIAL PRIMARY KEY,
        internal_id VARCHAR REFERENCES client_main( internal_id),
        fee_date DATE,
        aum_bps NUMERIC,
        additional_fee_bps NUMERIC,
        created_at DATE
    )
    """
)
conn.commit()

cur.execute("TRUNCATE client_fee;") # Truncate the table before loading the data

for file in os.listdir("./data/fees"):
    with open(f"./data/fees/{file}", "r") as f:
        next(f)             # Skip the header row.
        cur.copy_expert("COPY client_fee( \
                        internal_id, \
                        fee_date, \
                        aum_bps, \
                        additional_fee_bps, \
                        created_at) \
                        FROM STDIN CSV", f)
    conn.commit()
    print(f"{file} loaded")

# Close the cursor and connection
cur.close()
conn.close()
print("Connection closed")
