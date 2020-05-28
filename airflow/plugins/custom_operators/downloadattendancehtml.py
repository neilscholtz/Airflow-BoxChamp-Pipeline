from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.boxchamp import BoxChamp

class DownloadAttendanceHtmlOperator(BaseOperator):
    ui_color = '#7B9867'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(DownloadAttendanceHtmlOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        - Takes in the date the day before the execution date
        - Opens Chrome driver
        - Logs into BoxChamp and downloads an html file with attendance for that date
        - Closes Chrome driver
        '''
        self.log.info(context['yesterday_ds'])
        bc = BoxChamp()
        data = bc.get_class_attendance(context['yesterday_ds'])
        self.log.info('Following files have been saved:')
        for i in data:
            self.log.info(i)
        bc.close()