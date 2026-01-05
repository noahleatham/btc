import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    account='DNLBRLD-LIB75726',
    user='nleatham2',
    password='25",mtWhwnM8*a~',
    database='btc',
    warehouse='compute_wh',
    role='accountadmin',
    schema='btc_dev'
)

# Create cursor
cur = conn.cursor()

# Query the data
print("Total rows in stg_btc__btc:")
cur.execute('SELECT COUNT(*) FROM stg_btc__btc')
print(cur.fetchone()[0])

print("\nSample data (first 5 rows):")
cur.execute('SELECT hash_key, block_number, output_index, output_type, output_individual_value FROM stg_btc__btc LIMIT 5')
for row in cur.fetchall():
    print(row)

# Close connection
conn.close()