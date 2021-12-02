from db import DB
from mapbx import MapBx
from visuals import *
import csv
import asyncio
from cluster import Cluster
import time

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

    if(fresh_init): # CHANGE ASYNC TO SYNC 
        db.create_locations_table(locations_table_name)
        generator_csv = get_generator_csv('csvs/place_name_raw_data.csv', '\t', 1)
        db.insert_data_csv(locations_table_name, 'name', 9, generator_csv)
        generator_csv = get_generator_csv('csvs/population_by_0.csv', ',', 2)
        db.update_population_by_name_csv(locations_table_name, generator_csv)
        asyncio.run(db.update_location_center_async(locations_table_name, mpbx.get_location_center_async))

        db.create_durations_table(durations_table_name)
        locations = db.select_all_with_conditions(locations_table_name)
        asyncio.run(db.init_durations_table_async(durations_table_name, locations, mpbx.get_duration_async))

    c = Cluster()
    curs = db.select_locations_ids_with_conditions(locations_table_name)
    points = [point[0] for point in curs]
    durations = db.get_durations_pandas_df(durations_table_name)
    duration_list = [0.5, 1, 1,5, 3, 4]
    for duration in duration_list:
        start = time.time()
        df = c.cluster_by_duration(points, durations, duration, locations_table_name, db.get_location_info_by_id)
        show_locations(df, mpbx.get_token(), duration)
        end = time.time()
        print(f'Whitin {duration} Hours. Time Taken To Build: {end - start} sec.')

    # FIND INFO ABOUT LOCATION
    # df_loc = db.get_locations_pandas_df(locations_table_name)
    # df = df_loc.loc[df_loc['name'] == 'Ганцевичи']
    # print(df)

    # FIND DURATION BETWEEN LOCATIONS
    # dfdr = db.get_durations_pandas_df(durations_table_name)
    # df = dfdr.loc[(dfdr['a_location_id'] == 1) & (dfdr['b_location_id'] == 991)]
    # print(df)

    db.disconnect()

if __name__ == "__main__":
    main() 