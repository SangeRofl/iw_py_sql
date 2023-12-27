import os

import click
import mysql.connector
from dotenv import load_dotenv
import logging
import json

import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w")



class Model:
    def __init__(self):
        self.db_connector = None
        self.db_cursor = None

    def connect_to_db(self, password: str, host: str,
                      user: str, db_name: str) -> None:
        try:
            self.db_connector = mysql.connector.connect(host=host,
                                                    user=user,
                                                    password=password)
            logging.info("Connected to database's server")
        except Exception as e:
            logging.error("Could not connect to database's server", exc_info=True)
            return
        self.db_cursor = self.db_connector.cursor()
        self.db_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        self.db_cursor.execute(f"USE {db_name}")
        self.db_cursor.execute(f"""CREATE TABLE IF NOT EXISTS rooms
                                (id INT PRIMARY KEY,
                                 name VARCHAR(15))""")
        self.db_cursor.execute(f"""CREATE TABLE IF NOT EXISTS students
                                        (birthday DATE,
                                         id INT PRIMARY KEY,
                                         name VARCHAR(50),
                                         room INT,
                                         sex ENUM(\"F\", \"M\"),
                                         CONSTRAINT fk_rooms FOREIGN KEY (room) REFERENCES rooms(id))""")

    def clean_db(self):
        """Cleans database for next operations"""
        self.db_cursor.execute("DELETE FROM students")
        self.db_cursor.execute("DELETE FROM rooms")

    @staticmethod
    def get_json_file_data(filepath: str) -> dict | list:
        """Reads data from json file and returns data"""
        try:
            with open(filepath) as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Could not open json file: {filepath}")
            raise e

    def load_data_to_database(self, students_json_filepath: str, rooms_json_filepath: str) -> None:
        """Gets data from json files and fills database with it"""
        rooms = self.get_json_file_data(rooms_json_filepath)
        rooms = [tuple(i.values()) for i in rooms]
        self.db_cursor.executemany("INSERT INTO rooms (id, name) VALUES (%s, %s)", rooms)
        self.db_connector.commit()
        students = self.get_json_file_data(students_json_filepath)
        students = [tuple(i.values()) for i in students]
        self.db_cursor.executemany("INSERT INTO students (birthday, id, name, room, sex) VALUES (%s, %s, %s, %s, %s)", students)
        self.db_connector.commit()
        logging.info("Successfully loaded data to database.")

    def execute_queries_and_save(self, format: str) -> None:
        """Executes necessary queries and saves results in appropriate format"""
        self.execute_query_and_save("""SELECT r.name, COUNT(*)
         FROM rooms r 
         INNER JOIN students s 
         ON r.id = s.room
         GROUP BY r.id""", ["name", "count"], format, "query 1")

        self.execute_query_and_save("""SELECT r.name
                 FROM rooms r 
                 INNER JOIN students s 
                 ON r.id = s.room
                 GROUP BY r.id
                 ORDER BY AVG(DATEDIFF(NOW(), s.birthday))
                 LIMIT 5""", ["name"], format, "query 2")

        self.execute_query_and_save("""SELECT r.name
                         FROM rooms r 
                         INNER JOIN students s 
                         ON r.id = s.room
                         GROUP BY r.id
                         ORDER BY DATEDIFF(MAX(s.birthday), MIN(s.birthday)) DESC
                         LIMIT 5""", ["name"], format, "query 3")

        self.execute_query_and_save("""SELECT r.name
                                 FROM rooms r 
                                 INNER JOIN students s 
                                 ON r.id = s.room
                                 GROUP BY r.id
                                 HAVING COUNT(DISTINCT s.sex)=2""", ["name"], format, "query 4")

    def execute_query_and_save(self, query_text: str, col_names: list, save_format: str, query_name: str) -> None:
        """Executes query and saves result in appropriate format"""
        self.db_cursor.execute(query_text)
        logging.info(f"Query \"{query_name}\" executed successfully")
        data = self.db_cursor.fetchall()
        for i in range(len(data)):
            data[i] = dict(zip(col_names, data[i]))
        self.save_data_to_file(data, save_format, query_name)

    @staticmethod
    def save_data_to_json(data: list, filepath: str) -> None:
        """Saves data into json file"""
        try:
            with open(filepath, "w") as f:
                f.write(json.dumps(data))
            logging.info(f"Successfully saved data into json file: {filepath}")
        except Exception as e:
            logging.error(f"Could not write data into json file: {filepath}")
            raise e

    @staticmethod
    def save_data_to_xml(data: list, filepath: str) -> None:
        """Saves data into json file"""
        try:
            with open(filepath, "w") as f:
                root = ET.Element('data')
                for item in data:
                    entry = ET.SubElement(root, "entry")
                    for key, value in item.items():
                        sub_element = ET.SubElement(entry, key)
                        sub_element.text = str(value)
                tree = ET.ElementTree(root)
                tree.write(filepath, encoding="utf-8", xml_declaration=True)
            logging.info(f"Successfully saved data into xml file: {filepath}")
        except Exception as e:
            logging.error(f"Could not write data into xml file: {filepath}")
            raise e

    def save_data_to_file(self, data: list, format: str, filename: str) -> None:
        """Saves data in appropriate format"""
        filepath = "output\\"+filename+"."+format
        if "json" == format.lower():
            self.save_data_to_json(data, filepath)
        elif "xml" == format.lower():
            self.save_data_to_xml(data, filepath)
        else:
            logging.error("Unknown output file format")
            raise Exception


@click.command()
@click.argument("stud-filepath")
@click.argument("rooms-filepath")
@click.option("--file-format", "-f", default="json")
def main(stud_filepath, rooms_filepath, file_format):
    load_dotenv()
    m = Model()
    user = os.getenv("DB_USERNAME")
    host = os.getenv("DB_HOST")
    pwd = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    m.connect_to_db(pwd, host, user, db_name)
    m.clean_db()
    m.load_data_to_database(stud_filepath, rooms_filepath)
    m.execute_queries_and_save(file_format)


if __name__ == "__main__":
    main()
