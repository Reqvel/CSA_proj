from asyncio import tasks
from sqlite3.dbapi2 import Cursor
import sqlite3
import pandas as pd
from pandas.core.frame import DataFrame
import asyncio
import aiohttp

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

    
    def create_durations_table(self, table_name: str) -> None:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                a_location_id INTEGER NOT NULL,
                b_location_id INTEGER NOT NULL,
                duration_hours REAL
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


    async def update_location_center(self, table_name: str, get_lon_lat: callable) -> None:
        cursor = self._sql_connection.cursor()
        update_cursor = self._sql_connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE population IS NOT NULL")

        async with aiohttp.ClientSession() as session:
            tasks = []
            for row in cursor:
                id = row[0]
                location_name = row[1]
                task = asyncio.ensure_future(get_lon_lat(session, location_name, id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
        
        for id, longitude, latitude in results:
            if(longitude and latitude):
                update_cursor.execute(f"""
                    UPDATE {table_name}
                    SET longitude = ?, latitude = ?
                    WHERE id = ?
                """, (longitude, latitude, id))
        self._sql_connection.commit()

    
    async def init_durations_table(self, table_name: str, locations: Cursor, get_duration: callable):
        locations_list = locations.fetchall()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for a_location in locations_list:
                a_id = a_location[0]
                a_lon = a_location[2]
                a_lat = a_location[3]

                for b_location in locations_list:
                    b_id = b_location[0]
                    b_lon = b_location[2]
                    b_lat = b_location[3]

                    task = asyncio.ensure_future(get_duration(
                        session,
                        a_id,
                        a_lon,
                        a_lat,
                        b_id,
                        b_lon,
                        b_lat
                    ))
                    tasks.append(task)

            results = await asyncio.gather(*tasks)

        update_cursor = self._sql_connection.cursor()
        for a_id, b_id, duration in results:
            duration_hours = duration / 3600
            update_cursor.execute(f"""
                    UPDATE {table_name}
                    SET a_location_id = ?, b_location_id = ?, duration_hours = ?
                    WHERE id = ?
                """, (a_id, b_id, duration_hours))
        self._sql_connection.commit()


    def select_all_with_conditions(self, table_name: str) -> Cursor:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            SELECT *
            FROM {table_name} 
            WHERE (population and longitude and latitude) IS NOT NULL""")
        return cursor

    
    def select_all(self, table_name: str) -> Cursor:
        cursor = self._sql_connection.cursor()
        cursor.execute(f"""
            SELECT *
            FROM {table_name}""")
        return cursor

    
    def get_pandas_df(self, table_name: str) -> DataFrame:
        data_frame = pd.read_sql(f"""
        SELECT DISTINCT name, longitude, latitude, population
        FROM {table_name} 
        WHERE (population and longitude and latitude) IS NOT NULL""",
        self._sql_connection)

        return data_frame
