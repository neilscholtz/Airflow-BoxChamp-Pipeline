from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.boxchamp import BoxChamp

class DatabaseInsertOperator(BaseOperator):
    insert_sql='''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
        '''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 *args, **kwargs):

        super(DatabaseInsertOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.table=table

    def execute(self, context):
        '''
        - Takes in the date
        - Scrapes the attendance data
        - Saves each class as a seperate html file
        - Prints the filenames to the log
        '''
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        data = postgres.get_records(DatabaseInsertOperator.insert_sql)
        self.log.info('=============================================')
        self.log.info(data)
        self.log.info('=============================================')