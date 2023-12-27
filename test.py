import unittest
import main
import os
import dotenv
import mysql.connector

class Test(unittest.TestCase):
    def setUp(self) -> None:
        self.model = main.Model()
        dotenv.load_dotenv()
        self.user = os.getenv("DB_USERNAME")
        self.host = os.getenv("DB_HOST")
        self.pwd = os.getenv("DB_PASSWORD")
        self.db_name = os.getenv("DB_NAME")
        self.connector = mysql.connector.connect(host=self.host,
                                                 user=self.user,
                                                 password=self.pwd)
        self.db_cursor = self.connector.cursor()

    def test_db_connect_fail(self):
        self.model.connect_to_db("SDsadsa", "asdasdas", "dsadasd", "dsadsad")
        self.assertIsNone(self.model.db_cursor)
        self.assertIsNone(self.model.db_connector)

    def test_db_connect_success(self):
        self.model.connect_to_db(self.pwd, self.host, self.user, self.db_name)
        self.assertIsNotNone(self.model.db_cursor)
        self.assertIsNotNone(self.model.db_connector)

    def test_db_clean(self):
        self.model.connect_to_db(self.pwd, self.host, self.user, self.db_name)
        self.model.clean_db()
        self.db_cursor.execute(f"USE {self.db_name}")



if __name__ == "__main__":
    unittest.main()
