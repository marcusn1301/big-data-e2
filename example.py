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
                            id CHAR(3) PRIMARY KEY,
                            has_labels BOOL NOT NULL)
                            ''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS Activity (
                            id BIGINT NOT NULL,
                            user_id CHAR(3) NOT NULL,
                            transportation_mode CHAR(10),
                            start_date_time DATETIME,
                            end_date_time DATETIME,
                            PRIMARY KEY(id, user_id),
                            FOREIGN KEY (user_id) REFERENCES User(id))
                            ''') 
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS TrackPoint (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            activity_id BIGINT NOT NULL,
                            user_id CHAR(3) NOT NULL,
                            lat DOUBLE NOT NULL,
                            lon DOUBLE NOT NULL,
                            altitude INT NOT NULL,
                            date_days DOUBLE NOT NULL,
                            date_time DATETIME NOT NULL,
                            FOREIGN KEY (activity_id, user_id) REFERENCES Activity(id, user_id))
                            ''')
        
        self.db_connection.commit()

    def insert_data(self):

        labeled_ids = []

        with open(f'./dataset/labeled_ids.txt') as labeled_ids_file:
            lines = labeled_ids_file.read().splitlines()
            labeled_ids += list(map(lambda l: int(l), lines))
        
        for userID in filter(lambda u: u.isnumeric(), os.listdir('./dataset/Data')):
            hasLabel = int(userID) in labeled_ids
            
            # print('Inserting user', userID)
            
            self.cursor.execute(f'INSERT INTO User VALUES ({int(userID)}, {hasLabel})')

            # self.runQuery('SELECT * FROM Activity WHERE id=20081122143235', read=True)

            labels = []

            if hasLabel:
                with open(f'./dataset/Data/{userID}/labels.txt') as labelsFile:
                    lines = labelsFile.read().splitlines()[1:]

                    for line in lines:
                        words = line.split()

                        words[0] = words[0].replace('/', '-')
                        words[2] = words[2].replace('/', '-')

                        labels.append([' '.join(words[:2]), ' '.join(words[2:4]), words[4]])

            for activityID in map(lambda f: f.split('.')[0], os.listdir(f'./dataset/Data/{userID}/Trajectory')):
                with open(f'./dataset/Data/{userID}/Trajectory/{activityID}.plt') as pltFile:
                    lines = pltFile.read().splitlines()[6:]

                    # Skip filer med mer enn 2500 linjer
                    if len(lines) > 2500:
                        continue

                    startDateTime = ' '.join(lines[0].split(',')[-2:])
                    endDateTime = ' '.join(lines[-1].split(',')[-2:])

                    transportationMode = ''

                    for label in labels:
                        if label[0] == startDateTime and label[1] == endDateTime:
                            transportationMode = label[2]
                            break

                    self.cursor.execute(f'''INSERT INTO Activity VALUES 
                                        ({int(activityID)}, {int(userID)}, '{transportationMode}', '{startDateTime}', '{endDateTime}')''') 

                    # Sekvensiell insertion:
                    # for i, line in enumerate(lines):
                    #     fields = line.split(',')
                    #     self.cursor.execute(f'''INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES 
                    #                     ({int(activityID)}, {fields[0]}, {fields[1]}, {fields[2]}, '{fields[3]}', '{' '.join(fields[-2:])}')''') 
                    #     if i%100 == 0:
                    #         print(i)

                    # Batch insertion (MYE RASKERE):
                    query = 'INSERT INTO TrackPoint (activity_id, user_id, lat, lon, altitude, date_days, date_time) VALUES'
                    for line in lines:
                        if query.endswith(')'):
                            query += ','
                        fields = line.split(',')
                        query += f'''({int(activityID)}, {int(userID)}, {fields[0]}, {fields[1]}, {fields[2]}, '{fields[3]}', '{' '.join(fields[-2:])}')'''

                    self.cursor.execute(query)

            print(f'Inserted user {userID}')

        self.db_connection.commit()

    def runQuery(self, query, read=False):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.db_connection.commit()

        if read:
            print(rows)


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

        print(query % table_name)

        self.cursor.execute(query % table_name)

        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()

        # program.drop_table('TrackPoint')
        # program.drop_table('Activity')
        # program.drop_table('User')

        # program.show_tables()

        # program.create_table()
        # program.insert_data()
        # _ = program.fetch_data(table_name="User")
        # program.drop_table(table_name="TrackPoint")
        # # Check that the table is dropped

        program.runQuery('SELECT COUNT(id) FROM User;', read=True)
        program.runQuery('SELECT COUNT(id) FROM Activity;', read=True)
        program.runQuery('SELECT COUNT(id) FROM TrackPoint;', read=True)
        # [(182,)]
        # [(18669,)]
        # [(16234256,)]

        # print('Task 2')
        # program.runQuery('''
        # SELECT AVG(u.tCount), MIN(u.tCount), MAX(u.tCount) FROM 
        # (SELECT COUNT(TrackPoint.id) as tCount
        # FROM User
        # LEFT JOIN Activity ON Activity.user_id=User.id
        # LEFT JOIN TrackPoint ON TrackPoint.activity_id=Activity.id AND TrackPoint.user_id=Activity.user_id
        # GROUP BY User.id) u''', read=True)
        # [(Decimal('53196.4615'), 0, 1010325)]
        
        # print('Task 3')
        # program.runQuery('''
        # SELECT uId, tCount FROM 
        # (SELECT User.id as uId, COUNT(TrackPoint.id) as tCount
        # FROM User
        # LEFT JOIN Activity ON Activity.user_id=User.id
        # LEFT JOIN TrackPoint ON TrackPoint.activity_id=Activity.id AND TrackPoint.user_id=Activity.user_id
        # GROUP BY User.id) u
        # ORDER BY tCount DESC LIMIT 15''', read=True)
        # [('128', 1010325), ('153', 957841), ('25', 433501), ('163', 332364), ('41', 318169), ('68', 289605), ('4', 263603), ('62', 263455), ('85', 259612), ('17', 230085), ('14', 213801), ('3', 210728), ('144', 210031), ('167', 204842), ('30', 182984)]
        
        # print('Task 4')
        # program.runQuery('''
        # SELECT DISTINCT User.id FROM User
        # INNER JOIN Activity on Activity.user_id=User.id
        # WHERE Activity.transportation_mode='bus'
        # ''', read=True)
        # [('91',), ('175',), ('92',), ('10',), ('73',), ('125',), ('81',), ('62',), ('52',), ('112',), ('128',), ('85',), ('84',)]

        # print('Task 5')
        # program.runQuery(f'''
        # SELECT User.id, COUNT(DISTINCT Activity.transportation_mode) AS transportation_num
        # FROM User
        # INNER JOIN Activity ON Activity.user_id=User.id
        # GROUP BY User.id
        # ORDER BY transportation_num DESC
        # LIMIT 10
        # ''', read=True)
        # [('128', 10), ('62', 8), ('85', 5), ('84', 4), ('163', 4), ('112', 4), ('81', 4), ('78', 4), ('58', 4), ('102', 3)]
    

        # print('Task 6')
        # program.runQuery('''
        # SELECT Activity.id
        # FROM Activity
        # GROUP BY Activity.id
        # HAVING COUNT(1) > 1;
        # ''', read=True)
        # Resultatet e 12825 characters, det e rundt 675 activities dette gjelder

        # print('Task 7a')
        # program.runQuery('''
        # SELECT COUNT(DISTINCT User.id) as user_num
        # FROM User
        # INNER JOIN Activity ON Activity.user_id=User.id
        # WHERE DATE_ADD(DATE(start_date_time), INTERVAL 1 DAY) = DATE(end_date_time)
        # ''', read=True)
        # [(98,)]

        # program.runQuery('''
        # SELECT User.id, COUNT(Activity.id) as user_num
        # FROM User
        # INNER JOIN Activity ON Activity.user_id=User.id
        # WHERE DATE_ADD(DATE(start_date_time), INTERVAL 1 DAY) = DATE(end_date_time)
        # GROUP BY User.id
        # ''', read=True)

        # print('Task 7b')
        # program.runQuery('''
        # SELECT user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time)
        # FROM Activity
        # WHERE DATE_ADD(DATE(start_date_time), INTERVAL 1 DAY) = DATE(end_date_time)            
        # ''', read=True)
        # Resultatet her e 962 activities, se google docs

        print('Task 8')
        program.runQuery('''
        SELECT tp1.id, tp2.id, ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time))
        FROM TrackPoint AS tp1
        CROSS JOIN TrackPoint as tp2
        WHERE ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)) <= 30
        LIMIT 10
        ''', read=True)

        program.runQuery('''
        SELECT * 
        FROM TrackPoint
        WHERE TrackPoint.id = 11 OR TrackPoint.id = 1
        ''', read=True)

        #  DATE(start_date_time) < DATE(end_date_time) 
        #  AND DATEDIFF(end_date_time, start_date_time) = 1

        # Vi kan bruk manhattan distance på db nivå og haversine på python nivå

        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
