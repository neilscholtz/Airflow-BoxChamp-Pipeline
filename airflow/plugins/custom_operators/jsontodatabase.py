from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.boxchamp import *  
from helpers.db_functions import DataBase
import os
import configparser

class JsonToDatabaseOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(JsonToDatabaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        - Creates maps for member, coach_schedule, assistant_schedule, class_time, date and lead
        - For each JSON file for yesterday execution date, adds the attendance to database
        '''
        folder_config = configparser.ConfigParser()
        folder_config.read('/home/ec2-user/airflow/setup.cfg')
        download_folder = folder_config['FOLDERS']['HOME'] + folder_config['FOLDERS']['DOWNLOADS']
        
        json_files = [download_folder + file for file in os.listdir(download_folder) if context['yesterday_ds'] in file and '.json' in file]
        if len(json_files) > 0:
            db_config = configparser.ConfigParser()
            db_config.read('/home/ec2-user/db.cfg')
            db = DataBase(db_config['DB']['HOST'], db_config['DB']['DBNAME'], db_config['DB']['USERNAME'], db_config['DB']['PASSWORD'], db_config['DB']['PORT'])

            member_map = db.get_member_map()
            self.log.info('member_map retrieved.')
            coach_schedule_map = db.get_coach_schedule_map()
            self.log.info('coach_schedule map retrieved.')
            assistant_schedule_map = db.get_assistant_schedule_map()
            self.log.info('assistance_schedule map retrieved.')
            class_time_map = db.get_class_time_map()
            self.log.info('class_time map retrieved.')
            date_map = db.get_date_map()
            self.log.info('date_map map retrieved.')
            lead_map = db.get_lead_map()
            self.log.info('lead_map map retrieved.')

            self.log.info('Following files where processed:')
            for json_file in json_files:
                data = format_attendance_for_database(json_file, member_map, coach_schedule_map, assistant_schedule_map, class_time_map, date_map, lead_map)
                new_data = []
                for i in data:
                    new_data.append(i + i)
                self.log.info(new_data)
                db.multiple_insert_attendance(new_data)
                self.log.info(f'-> {json_file}')
        else:
            self.log.info('No files required processing.')
            