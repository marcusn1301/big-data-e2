from DbConnector import DbConnector
from tabulate import tabulate
import os

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self):
        # query = """CREATE TABLE IF NOT EXISTS %s (
        #            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        #            name VARCHAR(30))
        #         """
        # # This adds table_name to the %s variable and executes the query
        # self.cursor.execute(query % table_name)

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS User (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            has_labels BOOL NOT NULL)
                            ''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS Activity (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            user_id INT NOT NULL,
                            transportation_mode CHAR(10),
                            start_date_time DATETIME,
                            end_date_time DATETIME,
                            FOREIGN KEY (user_id) REFERENCES User(id))
                            ''') 
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS TrackPoint (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            activity_id INT NOT NULL,
                            lat DOUBLE NOT NULL,
                            lon DOUBLE NOT NULL,
                            altitude INT NOT NULL,
                            date_days DOUBLE NOT NULL,
                            date_time DATETIME NOT NULL,
                            FOREIGN KEY (activity_id) REFERENCES Activity(id))
                            ''')
        
        self.db_connection.commit()

    def insert_data(self):
        for userID in filter(lambda u: u.isnumeric(), os.listdir('./dataset/Data')):
            self.cursor.execute(f'INSERT IGNORE INTO User VALUES ({int(userID)}, FALSE)') #TODO: FIX FALSE VERDIEN HER

            for activityID in map(lambda f: f.split('.')[0], os.listdir(f'./dataset/Data/{userID}/Trajectory')):
                with open(f'./dataset/Data/{userID}/Trajectory/{activityID}.plt') as pltFile:
                    lines = pltFile.read().splitlines()[6:]

                    # Skip filer med mer enn 2500 linjer
                    if len(lines) > 2500:
                        continue

                    startDateTime = ' '.join(lines[0].split(',')[-2:])
                    endDateTime = ' '.join(lines[-1].split(',')[-2:])
                    self.cursor.execute(f'''INSERT IGNORE INTO Activity VALUES 
                                        ({int(activityID)}, {int(userID)}, '', '{startDateTime}', '{endDateTime}')''') 

                    # Sekvensiell insertion:
                    # for i, line in enumerate(lines):
                    #     fields = line.split(',')
                    #     self.cursor.execute(f'''INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES 
                    #                     ({int(activityID)}, {fields[0]}, {fields[1]}, {fields[2]}, '{fields[3]}', '{' '.join(fields[-2:])}')''') 
                    #     if i%100 == 0:
                    #         print(i)

                    # Batch insertion (MYE RASKERE):
                    query = 'INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES'
                    for line in lines:
                        if query.endswith(')'):
                            query += ','
                        fields = line.split(',')
                        query += f'''({int(activityID)}, {fields[0]}, {fields[1]}, {fields[2]}, '{fields[3]}', '{' '.join(fields[-2:])}')'''

                    self.cursor.execute(query)

            print(f'Inserted user {userID}')

        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_table()
        program.insert_data()
        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="TrackPoint")
        # # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
