from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from helpers.boxchamp import BoxChamp  
import helpers.boxchamp as boxchamp
from helpers.db_functions import DataBase
import os
import configparser

class LeadUpdateOperator(BaseOperator):
    ui_color = '#006ED1'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(LeadUpdateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        - Creates BoxChamp instance
        - Creates DataBase instance
        - Retrieves pending leads from BoxChamp
        - Retrieves pending leads from DataBase
        - Find the differences between BoxChamp and DataBase
        - Adds new leads in BoxChamp to DataBase
        - Updates pending leads to converted leads in DataBase
        '''
        bc = boxchamp.BoxChamp()
        db_config = configparser.ConfigParser()
        db_config.read('/home/ec2-user/db.cfg')
        db = DataBase(db_config['DB']['HOST'], db_config['DB']['DBNAME'], db_config['DB']['USERNAME'], db_config['DB']['PASSWORD'], db_config['DB']['PORT'])

        
        # create dataframes of current state of leads
        bc_pending_leads_df = bc.run_pending_leads()
        db_pending_leads_df = db.run_pending_leads()

        # create 2 lists of member_boxchamp_id's that need to be processed
        db_update_to_converted_lead, db_add_new_lead = boxchamp.lead_changes(bc_pending_leads_df, db_pending_leads_df)

        # check if pending leads are synced between boxchamp and database
        if boxchamp.leads_not_synced(db_pending_leads_df, bc_pending_leads_df):
            # adds new leads to postgress database
            boxchamp.add_new_leads_to_db(db, db_add_new_lead, bc_pending_leads_df)
            self.log.info('Added new leads to database.')
            
            # updates pending leads in database to converted and updates the lead_until date
            boxchamp.lead_update_to_converted_in_db(db, db_update_to_converted_lead, db_pending_leads_df)
            self.log.info('Updated pending leads to converted in database.')