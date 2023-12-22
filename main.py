import mysql.connector


class Model:
    def __init__(self):
        self.db_connector = None
        self.db_cursor = None

    def connect_to_db(self, password: str, host: str = "localhost",
                      user: str = "root", db_name: str = "STUDENTS") -> None:
        self.db_connector = mysql.connector.connect(host=host,
                                                    user=user,
                                                    password=password)
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




if __name__ == "__main__":
    m = Model()
    m.connect_to_db("321qaz")