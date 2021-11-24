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