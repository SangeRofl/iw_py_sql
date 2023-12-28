import json
import unittest
import mysql.connector
import dotenv
import os
import main


class Test(unittest.TestCase):
    def setUp(self):
        dotenv.load_dotenv()
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USERNAME")
        self.db_pwd = os.getenv("DB_PASSWORD")
        self.db_name = "test_"+os.getenv("DB_NAME")
        self.db_connector = mysql.connector.connect(host=self.db_host, user=self.db_user, password=self.db_pwd)
        self.db_cursor = self.db_connector.cursor()
        self.model = main.Model()

    def test_db_connection_success(self):
        self.model.connect_to_db(self.db_pwd, self.db_host, self.db_user, self.db_name)
        self.assertIsNotNone(self.model.db_connector)
        self.assertIsNotNone(self.model.db_cursor)

    def test_db_connection_fail(self):
        with self.assertRaises(Exception):
            self.model.connect_to_db("1", "1", "1", "1")

    def test_db_clean(self):
        self.model.connect_to_db(self.db_pwd, self.db_host, self.db_user, self.db_name)
        self.model.clean_db()
        self.db_cursor.execute(f"USE {self.db_name}")
        self.db_cursor.execute("SELECT * FROM students")
        studs = self.db_cursor.fetchall()
        self.assertFalse(studs)
        self.db_cursor.execute("SELECT * FROM rooms")
        rooms = self.db_cursor.fetchall()
        self.assertFalse(rooms)

    def test_db_load_data(self):
        self.model.connect_to_db(self.db_pwd, self.db_host, self.db_user, self.db_name)
        self.model.clean_db()
        self.model.load_data_to_database("test_data\\students.json", "test_data\\rooms.json")
        self.db_cursor.execute(f"USE {self.db_name}")
        self.db_cursor.execute("SELECT COUNT(*) FROM STUDENTS")
        self.assertEqual(self.db_cursor.fetchall()[0][0], 10000)
        self.db_cursor.fetchall()
        self.db_cursor.execute("SELECT COUNT(*) FROM ROOMS")
        self.assertEqual(self.db_cursor.fetchall()[0][0], 1000)
        self.db_cursor.fetchall()

    def test_execute_query_and_save(self):
        self.model.connect_to_db(self.db_pwd, self.db_host, self.db_user, self.db_name)
        self.model.clean_db()
        self.model.load_data_to_database("test_data\\students.json", "test_data\\rooms.json")
        self.db_cursor.execute(f"USE {self.db_name}")
        self.model.execute_query_and_save("SELECT COUNT(*) FROM STUDENTS", ["count"], "json", "test_query1")
        with open("output\\test_query1.json") as f:
            data = json.load(f)
            self.assertEqual(data[0]["count"], 10000)

    def tearDown(self):
        if self.model.db_connector:
            self.model.db_connector.close()
        self.db_cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        self.db_connector.commit()
        for filename in os.listdir("output"):
            if 'test_' in filename:
                file_path = os.path.join("output", filename)
                os.remove(file_path)


if __name__ == "__main__":
    unittest.main()
