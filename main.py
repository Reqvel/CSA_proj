from db import DB
from mapbx import MapBx
from visuals import *
import csv
import requests
import asyncio

def get_generator_csv(file_name, delimiter, skip_rows):
    with open(file_name, 'r', encoding="UTF8", errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        for _ in range(skip_rows):
            next(csv_reader)
        for line in csv_reader:
            yield line

def main():
    db_name = 'kurs_5sem.db'
    locations_table_name = 'locations'
    durations_table_name = 'durations'
    fresh_init = False

    db = DB(db_name)
    mpbx = MapBx('private.json', 'mapbox_access_token')
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    db.connect()

    if(fresh_init):
        db.create_locations_table(locations_table_name)
        generator_csv = get_generator_csv('csvs/place_name_raw_data.csv', '\t', 1)
        db.insert_data_csv(locations_table_name, 'name', 9, generator_csv)
        generator_csv = get_generator_csv('csvs/population_by_0.csv', ',', 2)
        db.update_population_by_name_csv(locations_table_name, generator_csv)
        asyncio.run(db.update_location_center(locations_table_name, mpbx.get_location_center))

        # db.create_durations_table(durations_table_name)
        # locations = db.select_all_with_conditions(locations_table_name)
        # asyncio.run(db.init_durations_table(durations_table_name, locations, mpbx.get_duration))

    
    # db.create_durations_table(durations_table_name)
    locations = db.select_all_with_conditions(locations_table_name)
    asyncio.run(db.init_durations_table(durations_table_name, locations, mpbx.get_duration))

    dur = db.select_all(durations_table_name).fetchall()
    print(dur)
    print(len(dur))


    # locations = db.select_all_with_conditions(locations_table_name)
    # asyncio.run(db.init_durations_table(durations_table_name, locations, mpbx.get_duration))

    # data_frame = db.get_pandas_df(locations_table_name)
    # show_locations(data_frame, mpbx.get_token())

    # db.disconnect()


    # cursor = sql_connection.cursor()
    # cursor.execute(f"""
    #     SELECT DISTINCT name, longitude, latitude, population
    #     FROM {locations_table_name} 
    #     WHERE name = 'Берёза'""")
    # print(cursor.fetchall())

    # with open('private.json', 'r') as openfile:
    #     json_object = json.load(openfile)
    # mapbox_access_token = json_object["mapbox_access_token"]
    # locations_list = db.select_all_with_conditions(locations_table_name).fetchall()
    # a_lon = locations_list[0][2]
    # a_lat = locations_list[0][3]
    # b_lon = locations_list[1][2]
    # b_lat = locations_list[1][3]
    # url = f"https://api.mapbox.com/optimized-trips/v1/mapbox/driving/{a_lon},{a_lat};{b_lon},{b_lat}?access_token={mpbx.get_token()}"
    # json_data = requests.get(url).json()
    # if(json_data):
    #     print(json_data['trips'][0]['duration'])

    db.disconnect()

if __name__ == "__main__":
    main() 