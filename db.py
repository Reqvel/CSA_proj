def create_locations_table(sql_connection, table_name):
    cursor = sql_connection.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            name TEXT,
            longitude REAL,
            latitude REAL,
            population INTEGER
        )
    """)
    sql_connection.commit()


def insert_data_csv(sql_connection, table_name, column, arr_i, generator_csv):
    cursor = sql_connection.cursor()
    for arr in generator_csv:
        cursor.execute(f"INSERT INTO {table_name} ({column}) VALUES (?)", (arr[arr_i],))
    sql_connection.commit()