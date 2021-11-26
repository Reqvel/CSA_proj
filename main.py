from db import DB
from mapbx import MapBx
from visuals import *
import csv

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
    fresh_init = False

    db = DB(db_name)
    mpbx = MapBx('private.json', 'mapbox_access_token')

    db.connect()

    if(fresh_init):
        db.create_locations_table(locations_table_name)
        generator_csv = get_generator_csv('csvs/place_name_raw_data.csv', '\t', 1)
        db.insert_data_csv(locations_table_name, 'name', 9, generator_csv)
        generator_csv = get_generator_csv('csvs/population_by_0.csv', ',', 2)
        db.update_population_by_name_csv(locations_table_name, generator_csv)
        db.update_location_center(locations_table_name, mpbx.get_location_center, info=False)

    db.disconnect()


    # cursor = sql_connection.cursor()
    # cursor.execute(f"""
    #     SELECT DISTINCT name, longitude, latitude, population
    #     FROM {locations_table_name} 
    #     WHERE name = 'Берёза'""")
    # print(cursor.fetchall())

    # with open('private.json', 'r') as openfile:
    #     json_object = json.load(openfile)
    # mapbox_access_token = json_object["mapbox_access_token"]
    # url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/Byaroza.json?country=by&access_token={mapbox_access_token}"
    # json_data = requests.get(url).json()
    # if(json_data and json_data['features']):
    #     for res in json_data['features']:
    #         print(res)
    #         print('\n\n')

if __name__ == "__main__":
    main() 