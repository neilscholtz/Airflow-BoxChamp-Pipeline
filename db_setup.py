from db_functions import DataBase
import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('/home/ec2-user/db.cfg')
    db = DataBase(config['DB']['HOST'], config['DB']['DBNAME'], config['DB']['USERNAME'], config['DB']['PASSWORD'], config['DB']['PORT'])

    # Create tables
    print ('Creating table "class_time"...')
    # Create class_time table
    db.execute('''
        CREATE TABLE IF NOT EXISTS class_time(
        class_time_id SERIAL PRIMARY KEY,
        class_time TIME
        )
    ''')
    print ('Completed.')

    # Create date table
    print ('Creating table "date"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS date(
        date_id SERIAL PRIMARY KEY,
        day_of_week VARCHAR(9),
        day INT2,
        month INT2,
        year INT2,
        quarter INT2,
        is_school_holiday VARCHAR(3),
        is_public_holiday VARCHAR(3),
        is_weekday VARCHAR(3)
        )
    ''')
    print ('Completed.')

    # Create member table
    print ('Creating table "member"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS member(
        member_id SERIAL PRIMARY KEY,
        member_first_name VARCHAR(50),
        member_last_name VARCHAR(150),
        member_email VARCHAR(255),
        member_phone VARCHAR(20),
        member_boxchamp_id INT8,
        member_since DATE,
        member_until DATE,
        is_current VARCHAR(3),
        membership_type VARCHAR(15),
        membership_period VARCHAR(15)
        )
    ''')
    print ('Completed.')

    # Create lead table
    print ('Creating table "lead"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS lead(
        lead_id SERIAL PRIMARY KEY,
        lead_first_name VARCHAR(50),
        lead_last_name VARCHAR(150),
        lead_email VARCHAR(255),
        lead_phone VARCHAR(20),
        lead_still_lead VARCHAR(3),
        lead_boxchamp_id INT8,
        lead_since DATE,
        lead_until DATE,
        lead_outcome VARCHAR(150)
        )
    ''')
    print ('Completed.')

    # Create coach table
    print ('Creating table "coach"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS coach(
        coach_id SERIAL PRIMARY KEY,
        coach_first_name VARCHAR(50),
        coach_last_name VARCHAR(150),
        coach_email VARCHAR(255),
        coach_phone VARCHAR(20)
        )
    ''')
    print ('Completed.')

    # Create attendance table
    print ('Creating table "attendance"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS attendance(
        attendance_id SERIAL PRIMARY KEY,
        member_id INT4 REFERENCES member(member_id),
        non_member_email VARCHAR(150),
        non_member_full_name VARCHAR(150),
        lead_id INT4 REFERENCES lead(lead_id),
        date_id INT4 REFERENCES date(date_id),
        class_time_id INT4 REFERENCES class_time(class_time_id),
        coach_id INT4 REFERENCES coach(coach_id),
        assist_coach_id INT4 REFERENCES coach(coach_id), 
        boxchamp_class_id INT4,
        created_at timestamp DEFAULT current_timestamp
        )
    ''')
    print ('Completed.')

    # Create coaches_schedule table
    print ('Creating table "coach_schedule"...')
    db.execute('''
        CREATE TABLE IF NOT EXISTS coach_schedule(
        schedule_id SERIAL PRIMARY KEY,
        schedule_day_of_week VARCHAR(3), 
        class_time_id INT4 REFERENCES class_time(class_time_id),
        coach_id INT4 REFERENCES coach(coach_id),
        coach_role VARCHAR(10),
        schedule_active VARCHAR(3),
        schedule_start DATE,
        schedule_end DATE
        )
    ''')
    print ('Completed.')
