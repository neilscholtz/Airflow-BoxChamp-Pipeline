from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from custom_operators.downloadattendancehtml import DownloadAttendanceHtmlOperator
from custom_operators.parseattendancehtml import ParseAttendanceHtmlOperator
from custom_operators.addnewmembers import DatabaseInsertOperator
from custom_operators.jsontodatabase import JsonToDatabaseOperator
from custom_operators.memberupdate import MemberUpdateOperator
from custom_operators.leadupdate import LeadUpdateOperator
import configparser

config = configparser.ConfigParser()
config.read('/home/ec2-user/setup.cfg')

default_args = {
    'owner': 'Ballistix',
    'depends_on_past':False,
    'start_date': datetime(2020, 5, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('attendance_dag2',
          default_args=default_args,
          description='Load and transform data for attendance from Boxchamp',
          schedule_interval='@daily' #'0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

update_members = MemberUpdateOperator(
    task_id='UpdateMembers',
    dag=dag,
    provide_context=True
)

update_leads = LeadUpdateOperator(
    task_id='UpdateLeads',
    dag=dag,
    provide_context=True
)

scrape_html_attendance = DownloadAttendanceHtmlOperator(
    task_id='DownloadAttendanceHtml',
    dag=dag,
    provide_context=True
)

parse_html_attendance = ParseAttendanceHtmlOperator(
    task_id='ParseAttendanceHtml',
    dag=dag,
    provide_context=True
)

json_to_database = JsonToDatabaseOperator(
    task_id='JsonToDatabase',
    dag=dag,
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [update_members, update_leads]
update_members >> scrape_html_attendance
update_leads >> scrape_html_attendance
scrape_html_attendance >> parse_html_attendance
parse_html_attendance >> json_to_database
json_to_database >> end_operator