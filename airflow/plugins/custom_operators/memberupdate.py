from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import helpers.boxchamp as boxchamp
from helpers.db_functions import DataBase
import os
import configparser

class MemberUpdateOperator(BaseOperator):
    ui_color = '#006ED1'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(MemberUpdateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        - Creates BoxChamp instance
        - Creates DataBase instance
        - Retrieves active members from BoxChamp
        - Retrieves active members from DataBase
        - Retrieves inactive members from DataBase
        - Find the differences between BoxChamp and DataBase
        - Adds new members in BoxChamp to DataBase
        - Updates inactive members to active members in DataBase
        - Updates active members to inactive members in DataBase
        '''
        bc = boxchamp.BoxChamp()
        db_config = configparser.ConfigParser()
        db_config.read('/home/ec2-user/db.cfg')
        db = DataBase(db_config['DB']['HOST'], db_config['DB']['DBNAME'], db_config['DB']['USERNAME'], db_config['DB']['PASSWORD'], db_config['DB']['PORT'])

        # create dataframes of current state of members
        bc_active_members_df = bc.run_active_members()
        db_active_members_df = db.run_active_members()
        db_inactive_members_df = db.run_inactive_members()

        # creates 3 lists of member_boxchamp_id's that need to be processed
        db_update_to_active_member, db_update_to_inactive_member, db_add_new_member = boxchamp.member_changes(bc_active_members_df, db_active_members_df, db_inactive_members_df)

        # check if active members are synched between boxchamp and database
        if boxchamp.members_not_synced(db_active_members_df, bc_active_members_df):
            # adds new members to postgress database
            boxchamp.add_new_members_to_db(db, db_add_new_member, bc_active_members_df)
            self.log.info('Added new members to database.')
            
            # updates member in database to active
            boxchamp.member_update_to_active_db(db, db_update_to_active_member, bc_active_members_df)
            self.log.info('Updated inactive to active members in database.')
            
            # updates member in database to inactive
            boxchamp.member_update_to_inactive_db(db, db_update_to_inactive_member, db_active_members_df)
            self.log.info('Updated active to inactive members in database.')