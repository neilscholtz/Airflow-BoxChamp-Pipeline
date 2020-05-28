from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.boxchamp import BoxChamp

class ParseAttendanceHtmlOperator(BaseOperator):
    ui_color = '#4C7D94'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(ParseAttendanceHtmlOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        - Takes in the date the day before the execution date
        - Parses the html files that have that date in the title
        - Saves parses data as json files
        '''
        bc = BoxChamp() 
        data = bc.run_parse_all_attendance_html(context['yesterday_ds'])
        self.log.info('Following files have been saved:')
        for i in data:
            self.log.info(i)
        bc.close() 