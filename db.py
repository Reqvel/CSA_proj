import pip._vendor.requests as requests
import time

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


def update_population_by_name_csv(sql_connection, table_name, generator_csv):
    cursor = sql_connection.cursor()
    for arr in generator_csv:
        cursor.execute(f"""
            UPDATE {table_name}
            SET population = ?
            WHERE name = ?
        """, (arr[3], arr[2]))
    sql_connection.commit()


def get_location_center(location_name):
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{location_name}.json?country=by&access_token=pk.eyJ1IjoicmVxdmVsIiwiYSI6ImNrd2F2dmc4eDE2ODUydXFtdnp1c3V1bnUifQ.Ag4Rol9WaA-ssGznVCUXVA"
    json_data = requests.get(url).json()
    if(json_data and json_data['features']):
        for res in json_data['features']:
            if(res['text'] == location_name):
                # print(f"place_name : {res['place_name']}")
                # print(f"text : {res['text']}")
                # print(f"center : {res['center']}")
                # print('\n\n')
                return res['center']
    # else: print("*** DATA IS NULL ***")
    return None


def insert_location_center(sql_connection, table_name):
    cursor = sql_connection.cursor()
    update_cursor = sql_connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name} WHERE population IS NOT NULL")
    for row in cursor:
        id = row[0]
        location_name = row[1]
        center = get_location_center(location_name)
        if(center):
            longitude = center[0]
            latitude = center[1]

            update_cursor.execute(f"""
                UPDATE {table_name}
                SET longitude = ?, latitude = ?
                WHERE id = ?
            """, (longitude, latitude, id))
        time.sleep(1)
    sql_connection.commit()