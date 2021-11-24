from db import *
import sqlite3
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
    sql_connection = sqlite3.connect(db_name)

    create_locations_table(sql_connection, locations_table_name)
    generator_csv = get_generator_csv('place_name_raw_data.csv', '\t', 1)
    insert_data_csv(sql_connection, locations_table_name, 'name', 9, generator_csv)

    sql_connection.close()

if __name__ == "__main__":
    main()