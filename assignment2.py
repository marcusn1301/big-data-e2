from DbConnector import DbConnector
from tabulate import tabulate
import os
from haversine import haversine

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
                        query += f'''({int(activityID)}, {int(userID)}, {fields[0]}, {fields[1]}, {fields[3]}, {fields[4]}, '{' '.join(fields[-2:])}')'''

                    self.cursor.execute(query)

            print(f'Inserted user {userID}')

            self.db_connection.commit()

    def runQuery(self, query, read=False, rtrn=False):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.db_connection.commit()

        if read:
            print(rows)

        if rtrn: 
            return rows


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

    def killAllProcesses(self):
        processes = self.runQuery("SHOW PROCESSLIST;", rtrn=True)

        pids = list(map(lambda p: p[0], processes))

        for pid in pids[1:-1]:
            # Første process e alltid en event scheduler, mens siste e dette queryet (SHOW PROCESSLIST)
            print('Killed pid', pid)
            self.runQuery(f'KILL {pid};')

def main():
    program = None
    try:
        program = ExampleProgram()

        # program.killAllProcesses()

        # program.drop_table('TrackPoint')
        # program.drop_table('Activity')
        # program.drop_table('User')
        # program.show_tables()
        # program.create_table()
        # program.insert_data()
        # program.show_tables()

        # _ = program.fetch_data(table_name="User")
        # program.drop_table(table_name="TrackPoint")
        # # Check that the table is dropped

        # print('Task 1')
        # program.runQuery('SELECT COUNT(id) FROM User;', read=True)
        # program.runQuery('SELECT COUNT(id) FROM Activity;', read=True)
        # program.runQuery('SELECT COUNT(id) FROM TrackPoint;', read=True)
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
        
        #Endret til activities
        # print('Task 3')
        # program.runQuery('''
        # SELECT uId, aCount FROM
        # (SELECT User.id as uID, COUNT(Activity.id) as aCount
        # FROM User
        # INNER JOIN Activity ON Activity.user_id=User.id
        # GROUP BY User.id) u
        # ORDER BY aCount DESC
        # LIMIT 15
        #  ''', read=True)
                
        
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

        # print('Task 8')
        # # Dette hadde gitt svaret om det var snakk om trackpoints, men det blir alt for mye.
        # # for i in range(1000):
        # #     print(i)
        # #     program.runQuery(f'''
        # #     SELECT tp1.user_id, tp2.user_id
        # #     FROM TrackPoint AS tp1
        # #     CROSS JOIN TrackPoint as tp2
        # #     WHERE ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)) <= 30 AND
        # #     ABS(tp1.lat-tp2.lat) * 10000000 / 90 < 50 AND 
        # #     ABS(tp1.lon-tp2.lon) * 10000000 / 90 < 50 AND
        # #     tp1.user_id != tp2.user_id
        # #     LIMIT 10 OFFSET {i*10}
        # #     ''', read=True)
        # #     # , ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)), ABS(tp1.lat-tp2.lat) * 1000000 / 90, ABS(tp1.lon-tp2.lon) * 1000000 / 90
        
        # # for userID in program.runQuery('SELECT * FROM User', rtrn=True):
        # #     program.runQuery(f'''
        # #     SELECT tp1.user_id, tp2.user_id
        # #     FROM TrackPoint AS tp1
        # #     CROSS JOIN TrackPoint as tp2
        # #     WHERE ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)) <= 30 AND
        # #     ABS(tp1.lat-tp2.lat) * 10000 / 90 < 50 AND 
        # #     ABS(tp1.lon-tp2.lon) * 10000 / 90 < 50 AND
        # #     tp1.user_id != tp2.user_id
        # #     LIMIT 100
        # #     ''', read=True)

        # # users = program.runQuery('SELECT * FROM User', rtrn=True)

        # # userIDs = list(map(lambda u: u[0], users))


        # uids = list(map(lambda u: u[0], program.runQuery('SELECT id from User', rtrn=True)))
        # closeUsers = []
        # uToTrackPoints = {}
        # for uid in uids:
        #     print('u1:', uid)
        #     tps = program.runQuery(f'SELECT lat, lon, date_time FROM TrackPoint WHERE user_id = {uid}', rtrn=True)
            
        #     # Convert tps to a nicer format
        #     tps = list(map(lambda tp: (tp[0], tp[1], int(tp[2].timestamp())), tps))

        #     print('Reformatted', uid)

        #     for uid2, tsToTPs in uToTrackPoints.items():
        #         print('u2:', uid2)
                
        #         for tp in tps:
        #             listOfTPs = tsToTPs.get(int(tp[2]//30)-1, []) + \
        #                         tsToTPs.get(int(tp[2]//30), []) + \
        #                         tsToTPs.get(int(tp[2]//30)+1, [])
                    
        #             if any(map(lambda tp2: abs(tp2[2] - tp[2]) <= 30 and haversine((tp[0], tp[1]), (tp2[0], tp2[1])) <= 50, listOfTPs)):
        #                 print('FoundMatch', uid, uid2)
        #                 closeUsers.append(uid)
        #                 closeUsers.append(uid2)
        #                 del uToTrackPoints[uid2]
        #                 break

        #             if uid2 in closeUsers:
        #                 break

        #         if uid in closeUsers:
        #             break
            
        #     if uid not in closeUsers:
        #         uToTrackPoints[uid] = {}

        #         for tp in tps:
        #             if int(tp[2]//30) in uToTrackPoints[uid]:
        #                 uToTrackPoints[uid][int(tp[2]//30)].append(tp)
        #             else:
        #                 uToTrackPoints[uid][int(tp[2]//30)] = [tp]
        # print('CloseUsers:', closeUsers)
        # Det returna etter 22 min: ['1', '0', '103', '10', '104', '101', '106', '105', '108', '107', '110', '109', '114', '113', '115', '111', '116', '100', '119', '11', '125', '112', '126', '12', '128', '117', '13', '124', '130', '122', '14', '135', '140', '134', '144', '127', '150', '146', '151', '121', '153', '102', '155', '131', '158', '15', '16', '142', '162', '157', '163', '129', '167', '136', '174', '164', '179', '17', '19', '18', '20', '165', '22', '2', '24', '23', '26', '25', '29', '27', '3', '28', '34', '30', '36', '35', '37', '32', '39', '38', '4', '33', '41', '40', '43', '42', '46', '169', '5', '44', '51', '173', '52', '152', '55', '168', '56', '175', '57', '47', '58', '138', '62', '176', '67', '45', '68', '54', '69', '145', '7', '6', '71', '159', '73', '64', '76', '161', '78', '170', '84', '66', '85', '53', '88', '70', '89', '166', '9', '8', '91', '181', '92', '81', '93', '82', '94', '61']


        # # program.runQuery('''
        # # SELECT DISTINCT tp1.user_id
        # # FROM User AS u
        # # WHERE EXISTS (SELECT 1
        # #     FROM TrackPoint as tp1
        # #     CROSS JOIN TrackPoint as tp2
        # #     ON tp1.user_id != tp2.user_id
        # #     WHERE tp1.user_id = u.id AND
        # #     ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)) <= 30 AND
        # #     ABS(tp1.lat-tp2.lat) < 0.00045 AND 
        # #     ABS(tp1.lon-tp2.lon) < 0.00045
        # # )
        # # ''', read=True)

        # # program.runQuery('''
        # # SELECT tp1.user_id
        # # FROM TrackPoint AS tp1
        # # CROSS JOIN TrackPoint as tp2
        # # WHERE tp1.user_id != tp2.user_id AND 
        # # ABS(TIMESTAMPDIFF(SECOND, tp2.date_time, tp1.date_time)) <= 30 AND
        # # ABS(tp1.lat-tp2.lat) * 10000000 / 90 < 50 AND 
        # # ABS(tp1.lon-tp2.lon) * 10000000 / 90 < 50
        # # GROUP BY tp1.user_id
        # # LIMIT 100
        # # ''', read=True)


        # #  DATE(start_date_time) < DATE(end_date_time) 
        # #  AND DATEDIFF(end_date_time, start_date_time) = 1

        # # INNER JOIN Activity ON Activity.user_id = User.id
        # # INNER JOIN TrackPoint ON TrackPoint.activity_id = Activity.id AND TrackPoint.user_id = Activity.user_id

        # # Vi kan bruk manhattan distance på db nivå og haversine på python nivå

        # print('Task 9')
        # program.runQuery('''
        # SELECT tp1.user_id, SUM(tp2.altitude - tp1.altitude) * 0.3048 AS altitudeGained
        # FROM TrackPoint AS tp1
        # INNER JOIN TrackPoint AS tp2
        # ON tp2.id = tp1.id + 1
        # WHERE tp1.altitude != -777 AND tp2.altitude != -777
        # AND tp2.user_id = tp1.user_id AND tp1.activity_id = tp2.activity_id
        # AND tp2.altitude > tp1.altitude
        # GROUP BY tp1.user_id
        # ORDER BY altitudeGained DESC
        # LIMIT 15
        # ''', read=True)

        # Dette returne ett 1:30: [('128', Decimal('650886.6840')), ('153', Decimal('554969.4768')), ('4', Decimal('332036.3184')), ('41', Decimal('240758.4720')), ('3', Decimal('233663.6424')), ('85', Decimal('217642.1352')), ('163', Decimal('205264.2072')), ('62', Decimal('181692.1944')), ('144', Decimal('179457.4008')), ('30', Decimal('175679.7096')), ('39', Decimal('146703.5928')), ('84', Decimal('131161.2312')), ('0', Decimal('121504.8624')), ('2', Decimal('115062.9144')), ('167', Decimal('112973.2056')), ('25', Decimal('109148.2704')), ('37', Decimal('99220.9344')), ('140', Decimal('94838.8248')), ('126', Decimal('83024.1672')), ('17', Decimal('62566.2960')), ('34', Decimal('61439.7552')), ('42', Decimal('61319.9688')), ('7', Decimal('60795.7128')), ('22', Decimal('60501.2760')), ('14', Decimal('60148.9272')), ('28', Decimal('54018.1800')), ('13', Decimal('50588.2656')), ('44', Decimal('48724.7184')), ('96', Decimal('46660.9176')), ('12', Decimal('46044.9168')), ('15', Decimal('45139.6608')), ('115', Decimal('44387.7192')), ('24', Decimal('44334.9888')), ('5', Decimal('43074.9456')), ('1', Decimal('39407.8968')), ('38', Decimal('39378.0264')), ('147', Decimal('37626.6456')), ('168', Decimal('36481.5120')), ('52', Decimal('34431.4272')), ('92', Decimal('34126.0176')), ('36', Decimal('32947.9656')), ('10', Decimal('32046.6720')), ('35', Decimal('31818.3768')), ('111', Decimal('31468.4664')), ('82', Decimal('31018.5816')), ('18', Decimal('30794.5536')), ('142', Decimal('30720.7920')), ('125', Decimal('30628.7424')), ('6', Decimal('29157.4728')), ('9', Decimal('28905.7080')), ('179', Decimal('26642.2632')), ('43', Decimal('26583.7416')), ('16', Decimal('25181.6616')), ('174', Decimal('24049.0248')), ('11', Decimal('22850.8560')), ('65', Decimal('22450.0440')), ('29', Decimal('22343.6688')), ('155', Decimal('22213.8240')), ('40', Decimal('22142.5008')), ('106', Decimal('20019.8736')), ('119', Decimal('18733.6176')), ('8', Decimal('17861.5848')), ('23', Decimal('17020.3368')), ('46', Decimal('16948.7088')), ('32', Decimal('15471.3432')), ('26', Decimal('15382.6464')), ('61', Decimal('15197.0232')), ('78', Decimal('15108.9360')), ('67', Decimal('13314.5784')), ('19', Decimal('13179.2472')), ('101', Decimal('13112.1912')), ('81', Decimal('11222.4312')), ('169', Decimal('11121.8472')), ('130', Decimal('9487.2048')), ('112', Decimal('9207.3984')), ('113', Decimal('8978.1888')), ('64', Decimal('8777.0208')), ('105', Decimal('8756.9040')), ('63', Decimal('7837.9320')), ('158', Decimal('7617.2568')), ('73', Decimal('7563.0024')), ('150', Decimal('7371.8928')), ('57', Decimal('7371.8928')), ('94', Decimal('7371.8928')), ('134', Decimal('7039.3560')), ('89', Decimal('6906.4632')), ('122', Decimal('6114.8976')), ('99', Decimal('5948.1720')), ('138', Decimal('5935.0656')), ('123', Decimal('5926.2264')), ('79', Decimal('5824.7280')), ('88', Decimal('5382.4632')), ('145', Decimal('5202.6312')), ('139', Decimal('5097.4752')), ('121', Decimal('4993.2336')), ('21', Decimal('4495.1904')), ('157', Decimal('4481.4744')), ('135', Decimal('4404.0552')), ('50', Decimal('4403.7504')), ('172', Decimal('4291.8888')), ('166', Decimal('4256.8368')), ('181', Decimal('4207.4592')), ('154', Decimal('4163.5680')), ('118', Decimal('3935.8824')), ('95', Decimal('3819.7536')), ('103', Decimal('3692.9568')), ('136', Decimal('3489.6552')), ('70', Decimal('3375.9648')), ('56', Decimal('3192.4752')), ('55', Decimal('3107.7408')), ('97', Decimal('2777.0328')), ('33', Decimal('2763.6216')), ('108', Decimal('2742.8952')), ('161', Decimal('2709.0624')), ('124', Decimal('2585.0088')), ('45', Decimal('2537.4600')), ('176', Decimal('2515.5144')), ('98', Decimal('2420.7216')), ('170', Decimal('2314.6512')), ('127', Decimal('2075.9928')), ('77', Decimal('2043.0744')), ('164', Decimal('1986.0768')), ('58', Decimal('1939.1376')), ('129', Decimal('1836.1152')), ('175', Decimal('1826.3616')), ('27', Decimal('1724.5584')), ('47', Decimal('1717.2432')), ('100', Decimal('1558.1376')), ('69', Decimal('1508.7600')), ('80', Decimal('1502.3592')), ('76', Decimal('1489.2528')), ('54', Decimal('1404.2136')), ('146', Decimal('1364.8944')), ('152', Decimal('1317.6504')), ('107', Decimal('1306.9824')), ('86', Decimal('1112.8248')), ('75', Decimal('1112.2152')), ('162', Decimal('1093.6224')), ('91', Decimal('1082.3448')), ('53', Decimal('1009.8024')), ('66', Decimal('971.0928')), ('171', Decimal('910.4376')), ('117', Decimal('736.7016')), ('90', Decimal('729.3864')), ('151', Decimal('716.5848')), ('116', Decimal('586.1304')), ('31', Decimal('569.9760')), ('87', Decimal('397.4592')), ('114', Decimal('395.3256')), ('72', Decimal('360.5784')), ('48', Decimal('360.2736')), ('180', Decimal('333.7560')), ('60', Decimal('320.0400')), ('133', Decimal('301.4472')), ('93', Decimal('285.5976')), ('178', Decimal('22.8600'))]

        # Queryet forutsette at TrackPoint alltid settes inn i rett rekkefølge, som vi tror er en fornuftig anntakelse. 
        
        print('Task 10')
        # Find the users that have traveled the longest total distance in one day for each
        # transportation mode.
        # program.runQuery('''
        # SELECT 
        #     Activity.user_id, Activity.transportation_mode, TIMEDIFF(Activity.end_date_time, Activity.start_date_time)
        # FROM
        #     Activity
        # INNER JOIN 
        #     TrackPoint 
        # ON 
        #     TrackPoint.activity_id = Activity.id AND TrackPoint.user_id = Activity.user_id
        # WHERE 
        #     DATE(start_date_time) = DATE(end_date_time) AND transportation_mode != ''
        # LIMIT 10
        # ''', read=True)
        
        # program.runQuery('''
        # SELECT 
        #     Activity.user_id, Activity.transportation_mode, TIMEDIFF(Activity.end_date_time, Activity.start_date_time)
        # FROM
        #     SELECT (
        # INNER JOIN 
        #     TrackPoint 
        # ON 
        #     TrackPoint.activity_id = Activity.id AND TrackPoint.user_id = Activity.user_id
        # WHERE 
        #     DATE(start_date_time) = DATE(end_date_time) AND transportation_mode != ''
        # GROUP BY Activity.id)
        # LIMIT 10
        # ''', read=True)

        # print('Task 12')

        # program.runQuery('''
        # SELECT id, 
        #     (
        #         SELECT transportation_mode 
        #         FROM Activity
        #         WHERE User.id = Activity.user_id
        #         AND Activity.transportation_mode != ''
        #         GROUP BY transportation_mode
        #         ORDER BY COUNT(Activity.id) DESC
        #         LIMIT 1
        #     ) AS mostUsedTransportationMode
        # FROM User
        # GROUP BY User.id
        # HAVING mostUsedTransportationMode IS NOT NULL
        # ORDER BY id ASC
        # ''', read=True)

        # Returne: [('10', 'taxi'), ('101', 'car'), ('102', 'bike'), ('107', 'walk'), ('108', 'walk'), ('111', 'taxi'), ('112', 'walk'), ('115', 'car'), ('117', 'walk'), ('125', 'bike'), ('126', 'bike'), ('128', 'car'), ('136', 'walk'), ('138', 'bike'), ('139', 'bike'), ('144', 'walk'), ('153', 'walk'), ('161', 'walk'), ('163', 'bike'), ('167', 'bike'), ('175', 'bus'), ('20', 'bike'), ('21', 'walk'), ('52', 'bus'), ('56', 'bike'), ('58', 'taxi'), ('60', 'walk'), ('62', 'walk'), ('64', 'bike'), ('65', 'bike'), ('67', 'walk'), ('69', 'bike'), ('73', 'walk'), ('75', 'walk'), ('76', 'car'), ('78', 'walk'), ('80', 'taxi'), ('81', 'bike'), ('82', 'walk'), ('84', 'walk'), ('85', 'walk'), ('86', 'car'), ('87', 'walk'), ('89', 'car'), ('91', 'bus'), ('92', 'bus'), ('97', 'bike'), ('98', 'taxi')]

        # program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
