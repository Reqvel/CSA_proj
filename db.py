from sqlite3.dbapi2 import Cursor
import sqlite3

class DB:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self._sql_connection = None


    def connect(self) -> None:
        self._sql_connection = sqlite3.connect(self.db_name)
    

    def disconnect(self) -> None:
        self._sql_connection.close()


    def create_locations_table(self, table_name: str) -> None:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT,
                longitude REAL,
                latitude REAL,
                population INTEGER
            )
        """)
        self._sql_connection.commit()


    def insert_data_csv(self, table_name: str, column: str, arr_i: int, generator_csv) -> None:
        cursor = self._sql_connection.cursor()
        for arr in generator_csv:
            cursor.execute(f"INSERT INTO {table_name} ({column}) VALUES (?)", (arr[arr_i],))
        self._sql_connection.commit()


    def update_population_by_name_csv(self, table_name: str, generator_csv) -> None:
        cursor = self._sql_connection.cursor()
        for arr in generator_csv:
            cursor.execute(f"""
                UPDATE {table_name}
                SET population = ?
                WHERE name = ?
            """, (arr[3], arr[2]))
        self._sql_connection.commit()


    def update_location_center(self, table_name: str, get_lon_lat: callable, info: bool=False) -> None:
        cursor = self._sql_connection.cursor()
        update_cursor = self._sql_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE population IS NOT NULL")
        for row in cursor:
            id = row[0]
            location_name = row[1]
            longitude, latitude = get_lon_lat(location_name, info)
            if(longitude and latitude):
                update_cursor.execute(f"""
                    UPDATE {table_name}
                    SET longitude = ?, latitude = ?
                    WHERE id = ?
                """, (longitude, latitude, id))
        self._sql_connection.commit()


    def select_all_with_conditions(self, table_name: str) -> Cursor:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            SELECT DISTINCT name, longitude, latitude, population
            FROM {table_name} 
            WHERE (population and longitude and latitude) IS NOT NULL""")
        return cursor

    
    def select_all(self, table_name: str) -> Cursor:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            SELECT *
            FROM {table_name}""")
        return cursor
