import psycopg2
import pandas as pd

class DataBase:
    def __init__(self, host, dbname, user, password, port):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
    
    def __connect__(self):
        """Opens connector class and initiates cursor"""
        self.con = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, 
                                                 database=self.dbname)
        self.cur = self.con.cursor()

    def __disconnect__(self):
        """Commits any changes to the database and closes connection"""
        self.con.commit()
        self.con.close()

    def fetch(self, sql, variables=None):
        """Connects to database, fetches data specific to sql query, then disconnects from database"""
        self.__connect__()
        try:
            self.cur.execute(sql, variables)
            result = self.cur.fetchall()
            return result
        except Exception as e:
            print (e)
        finally:
            self.__disconnect__()
        

    def execute(self, sql, variables=None):
        """Connects to database, executes sql query, along with any variables, then disconnects from database"""
        self.__connect__()
        try:
            self.cur.execute(sql, variables)
        except Exception as e:
            print (e)
        finally:
            self.__disconnect__()
            
    def get_cols(self, table, details='no'):
        data = self.fetch("""
                SELECT *
          FROM information_schema.columns
        where table_schema = 'public'
             ;
                """)
        cols = []
        if details == 'yes':
            for i in data:
                if i[2:][0] == table:
                    print (i[2:][1], i[2:][5], i[2:][6])
        else:
            for i in data:
                if i[2:][0] == table:
                    cols.append(i[2:][1])
            str_cols = ', '.join(cols)
            return str_cols
    
    def get_tables(self):
        data = self.fetch('''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
            ''')
        formatted_data = []
        for item in data:
            formatted_data.append(item[0])
        return formatted_data
            
    def single_insert_class_time(self, time):
        # for single entry inserts
        sql = '''
        INSERT INTO class_time (class_time)
        VALUEs (%s)
        '''
        variables = time
        self.execute(sql, (variables,))
        
    def multiple_insert_class_time(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO class_time (class_time)
                VALUES (%s)
                '''
                variables = row
                self.cur.execute(sql, (variables,))
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()
    
    def single_insert_date(self, day_of_week, day, month, year, quarter, is_school_holiday, is_public_holiday, is_weekday):
        # for single entry inserts
        sql = '''
        INSERT INTO date (day_of_week, day, month, year, quarter, is_school_holiday, is_public_holiday, is_weekday)
        VALUEs (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        variables = (day_of_week, day, month, year, quarter, is_school_holiday, is_public_holiday, is_weekday)
        self.execute(sql, variables)
        
    def multiple_insert_date(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO date (day_of_week, day, month, year, quarter, is_school_holiday, is_public_holiday, is_weekday)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()
            
    def single_insert_member(self, member_first_name, member_last_name, member_email, member_phone, member_boxchamp_id, member_since, member_until, is_current, membership_type, membership_period):
        # for single entry inserts
        sql = '''
        INSERT INTO member (member_first_name, member_last_name, member_email, member_phone, member_boxchamp_id, member_since, member_until, is_current, membership_type, membership_period)
        VALUEs (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        variables = (member_first_name, member_last_name, member_email, member_phone, member_boxchamp_id, member_since, member_until, is_current, membership_type, membership_period)
        self.execute(sql, variables)
        
    def multiple_insert_member(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO member (member_first_name, member_last_name, member_email, member_phone, member_boxchamp_id, member_since, member_until, is_current, membership_type, membership_period)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()

    def multiple_member_update_to_active(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                SELECT member_id
                FROM member
                WHERE member_boxchamp_id = %s
                '''
                variables = (row[4],)
                self.cur.execute(sql, variables)
                member_id = self.cur.fetchone()
                sql = '''
                UPDATE member
                SET member_first_name = %s,
                    member_last_name = %s,
                    member_email = %s,
                    member_phone = %s,
                    member_until = %s,
                    is_current = %s,
                    membership_type = %s,
                    membership_period = %s
                WHERE member_id = %s
                '''
                variables = (row[0], row[1], row[2], row[3], row[6], row[7], row[8], row[9], member_id)
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()

    def multiple_member_update_to_inactive(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                UPDATE member
                SET member_until = %s,
                    is_current = 'no'
                WHERE member_id = %s
                '''
                variables = (row[2], row[0])
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()

            
    def single_insert_lead(self, lead_first_name, lead_last_name, lead_email, lead_phone, lead_still_lead, lead_boxchamp_id, lead_since, lead_until, lead_outcome):
        # for single entry inserts
        sql = '''
        INSERT INTO lead (lead_first_name, lead_last_name, lead_email, lead_phone, lead_still_lead, lead_boxchamp_id, lead_since, lead_until, lead_outcome)
        VALUEs (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        variables = (lead_first_name, lead_last_name, lead_email, lead_phone, lead_still_lead, lead_boxchamp_id, lead_since, lead_until, lead_outcome)
        self.execute(sql, variables)
        
    def multiple_insert_lead(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO lead (lead_first_name, lead_last_name, lead_email, lead_phone, lead_still_lead, lead_boxchamp_id, lead_since, lead_until, lead_outcome)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()

    def multiple_lead_update_to_converted(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                UPDATE lead
                SET lead_still_lead = 'no',
                    lead_until = %s,
                    lead_outcome = 'converted'
                WHERE lead_id = %s
                '''
                variables = (row[8], row[0])
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()
            
    def single_insert_coach(self, coach_first_name, coach_last_name, coach_email, coach_phone):
        # for single entry inserts
        sql = '''
        INSERT INTO coach (coach_first_name, coach_last_name, coach_email, coach_phone)
        VALUEs (%s, %s, %s, %s)
        '''
        variables = (coach_first_name, coach_last_name, coach_email, coach_phone)
        self.execute(sql, variables)
        
    def multiple_insert_coach(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO coach (coach_first_name, coach_last_name, coach_email, coach_phone)
                VALUES (%s, %s, %s, %s)
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()
            
    def single_insert_attendance(self, member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id):
        # for single entry inserts
        sql = '''
        INSERT INTO attendance (member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id)
        VALUEs (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        variables = (member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id)
        self.execute(sql, variables)
        
    def multiple_insert_attendance(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO attendance (member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                SELECT member_id FROM attendance
                WHERE (member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id) = (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                )
                
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
            raise 
        finally:
            self.con.close()

    def single_insert_coach_schedule(self, schedule_day_of_week, class_time_id, coach_id, coach_role, schedule_active, schedule_start, schedule_end):
        # for single entry inserts
        sql = '''
        INSERT INTO coach_schedule (schedule_day_of_week, class_time_id, coach_id, coach_role, schedule_active, schedule_start, schedule_end)
        VALUEs (%s, %s, %s, %s, %s, %s, %s)
        '''
        variables = (schedule_day_of_week, class_time_id, coach_id, coach_role, schedule_active, schedule_start, schedule_end)
        self.execute(sql, variables)

    def multiple_insert_coach_schedule(self, data_list):
        # for multiple entry inserts
        self.__connect__()
        try:
            for row in data_list:
                sql = '''
                INSERT INTO coach_schedule (schedule_day_of_week, class_time_id, coach_id, coach_role, schedule_active, schedule_start, schedule_end)
        VALUEs (%s, %s, %s, %s, %s, %s, %s)
                '''
                variables = row
                self.cur.execute(sql, variables)
            self.con.commit()
        except Exception as e:
            print (e)
        finally:
            self.con.close()
            
    def get_member_map(self):
        '''
        Return dictionary of member_boxchamp_id as key and list of [member_id, member_first_name, member_last_name] as values
        '''
        sql = '''
            SELECT member_boxchamp_id, member_id, member_first_name, member_last_name
            FROM member
        '''
        data = self.fetch(sql)
        data_dict = {}
        for item in data:
            data_dict[item[0]] = {'member_id':item[1], 'member_full_name':item[2] + ' ' + item[3]}
        return data_dict

    def get_coach_map(self):
        '''
        Return dictionary of coach_id as key and list of [member_id, member_first_name, member_last_name] as values
        '''
        sql = '''
            SELECT coach_first_name, coach_last_name, coach_id
            FROM coach
        '''
        data = self.fetch(sql)
        data_dict = {}
        for item in data:
            data_dict[item[0]] = {'coach_id':item[2], 'coach_last_name':item[1]}
        return data_dict
    
    def get_coach_schedule_map(self):
        '''
        Return dictionary of to map correct coach to the class session
        '''
        data = self.fetch('''
        SELECT schedule_day_of_week, class_time.class_time, coach_id
        FROM coach_schedule
        INNER JOIN class_time ON coach_schedule.class_time_id = class_time.class_time_id
        WHERE coach_role = 'lead'
        AND schedule_active = 'yes'

        ''')

        data_dict = {'Mon':{}, 'Tue':{}, 'Wed':{}, 'Thu':{}, 'Fri':{}, 'Sat':{}, 'Sun':{}}
        for item in data:
            hour = item[1].hour
            minute = item[1].minute
            if hour > 12:
                suffix = 'pm'
            else:
                suffix = 'am'
            if len(str(hour)) < 2:
                hour = '0' + str(hour)
            else:
                hour = str(hour)
            if len(str(minute)) < 2:
                minute = '0' + str(minute)
            else:
                minute = str(minute)
            string_time = hour + minute + suffix
            data_dict[item[0]][string_time] = item[2]
        
        return data_dict

    def get_assistant_schedule_map(self):
        '''
        Return dictionary of to map correct assistant coach to the class session
        '''
        data = self.fetch('''
        SELECT schedule_day_of_week, class_time.class_time, coach_id
        FROM coach_schedule
        INNER JOIN class_time ON coach_schedule.class_time_id = class_time.class_time_id
        WHERE coach_role = 'assistant'
        AND schedule_active = 'yes'

        ''')

        data_dict = {'Mon':{}, 'Tue':{}, 'Wed':{}, 'Thu':{}, 'Fri':{}, 'Sat':{}, 'Sun':{}}
        for item in data:
            hour = item[1].hour
            minute = item[1].minute
            if hour > 12:
                suffix = 'pm'
            else:
                suffix = 'am'
            if len(str(hour)) < 2:
                hour = '0' + str(hour)
            else:
                hour = str(hour)
            if len(str(minute)) < 2:
                minute = '0' + str(minute)
            else:
                minute = str(minute)
            string_time = hour + minute + suffix
            data_dict[item[0]][string_time] = item[2]
        
        return data_dict

    def get_class_time_map(self):
        '''
        Return dictionary of string_time ('0500am') as key and class_time_id as value
        '''
        sql = '''
            SELECT class_time, class_time_id
            FROM class_time
            ORDER By class_time_id
        '''
        data = self.fetch(sql)
        data_dict = {}
        for item in data:
            hour_string = str(item[0].hour)
            minute_string = str(item[0].minute)
            if len(hour_string) < 2:
                hour_string = '0' + hour_string
            if len(minute_string) < 2:
                minute_string = minute_string + '0'
            if item[0].hour < 13:
                am_pm = 'am'
            else:
                am_pm = 'pm'
            string_time = hour_string + minute_string + am_pm
            data_dict[string_time] = {'class_time_id':item[1]}
        return data_dict
    
    def get_date_map(self):
        '''
        Return dictionary of date (datetime.date object) as key and date_id as value
        '''
        sql = '''
            SELECT date_id, year, month, day
            FROM date
            ORDER BY date_id
        '''
        data = self.fetch(sql)
        data_dict = {}
        for item in data:
            date_time = str(item[1]) + ',' + str(item[2]) + ',' + str(item[3])
            data_dict[date_time] = {'date_id':item[0]}
        return data_dict
    
    def get_lead_map(self):
        '''
        Return dictionary of lead_email as key and lead_id as value
        '''
        sql = '''
            SELECT lead_email, lead_id
            FROM lead
            ORDER By lead_id
        '''
        data = self.fetch(sql)
        data_dict = {}
        for item in data:
            data_dict[item[0]] = {'lead_id':item[1]}
        return data_dict
    
    def run_active_members(self):
        '''
        Returns DataFrame of active members
        '''
        sql = '''
        SELECT member_id, member_boxchamp_id 
        FROM member
        WHERE is_current = 'yes'
        '''
        data = self.fetch(sql)
        df = pd.DataFrame(data, columns=['member_id', 'member_boxchamp_id'])
        return df

    def run_inactive_members(self):
        '''
        Returns DataFrame of active members
        '''
        sql = '''
        SELECT member_id, member_boxchamp_id 
        FROM member
        WHERE is_current = 'no'
        '''
        data = self.fetch(sql)
        df = pd.DataFrame(data, columns=['member_id', 'member_boxchamp_id'])
        return df

    def run_pending_leads(self):
        '''
        Return DataFrame of pending leads
        '''
        sql = '''
            SELECT *
            FROM lead
            WHERE lead_outcome = 'pending'
        '''
        data = self.fetch(sql)
        columns = ['lead_id', 'lead_first_name', 'lead_last_name', 'lead_email', 'lead_phone', 'lead_still_lead', 'lead_boxchamp_id', 'lead_since', 'lead_until', 'lead_outcome']
        df = pd.DataFrame(data, columns=columns)
        return df

    def run_converted_leads(self):
        '''
        Return DataFrame of converted leads
        '''
        sql = '''
            SELECT *
            FROM lead
            WHERE lead_outcome = 'converted'
        '''
        data = self.fetch(sql)
        columns = ['lead_id', 'lead_first_name', 'lead_last_name', 'lead_email', 'lead_phone', 'lead_still_lead', 'lead_boxchamp_id', 'lead_since', 'lead_until', 'lead_outcome']
        df = pd.DataFrame(data, columns=columns)
        return df

if __name__ == "__main__":
    pass
