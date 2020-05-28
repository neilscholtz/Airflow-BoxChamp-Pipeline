import requests
import re
import time
import datetime
from bs4 import BeautifulSoup as bs4
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import pandas as pd
import numpy as np
import json
import configparser


# instantiate a chrome options object so you can set the size and headless preference
class BoxChamp:
    def __init__(self):
        self.config_urls = configparser.ConfigParser()
        self.config_urls.read('/home/ec2-user/airflow/boxchamp_urls.cfg')
        self.config_setup = configparser.ConfigParser()
        self.config_setup.read('/home/ec2-user/airflow/setup.cfg')
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument("--headless")
        options.add_argument("window-size=1980,1080")
        current_dir = os.getcwd()#.replace('\\','/')
        self.download_folder = self.config_setup['FOLDERS']['HOME'] + self.config_setup['FOLDERS']['DOWNLOADS']
        prefs = {'download.default_directory' : self.download_folder}
        options.add_experimental_option('prefs', prefs)
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        self.driver = webdriver.Chrome(options=options)
        self.database_folder = self.config_setup['FOLDERS']['HOME'] + self.config_setup['FOLDERS']['PROCESSING']
        # logins into boxchamp
        username = self.config_setup['BOXCHAMP_CREDENTIALS']['USERNAME']
        password = self.config_setup['BOXCHAMP_CREDENTIALS']['PASSWORD']
        try:
            self.driver.get(self.config_urls['URLS']['LOGIN'])
            self.driver.find_element_by_id ('txEmail').send_keys(username)
            self.driver.find_element_by_id ('txPassword').send_keys(password)
            self.driver.find_element_by_id ('btnLogin').click()
            self.driver.implicitly_wait(5)
        except:
            print ('ERROR: Cannot login...')
            self.driver.close()
            
        params = {'txEmail':username,'txPassword':password}
        session = requests.Session()
        session.post(self.config_urls['URLS']['LOGIN'],params)
        self.requests_session = session
    
    def close(self): # logs out boxchamp
        self.driver.close()
    
    def get_week_attendance(self, select_dates):
        """
        Takes a list of dates and saves all attendance across all classes for that the 
        Args:
            select_date (list): list of datetime objects
        Returns:
            saved_files (list): filename in which all the data is saved
        """
        filename = 'boxchamp_attenance_urls.csv'
        for select_date in select_dates:
            string_date = select_date#.strftime('%Y-%m-%d')
            date_attendance_url = '{}?date={}'.format(self.config_urls['URLS']['CLASS_ATTENDANCE'], string_date)
            html = self.requests_session.get(date_attendance_url).text
            soup = bs4(html, 'lxml')
            table = soup.find('div', {'class':'class-list hidden-for-small-only '})
            days = table.findAll('div', {'class':'class-list-day'})

            if filename not in os.listdir():
                with open(filename, 'w') as f:
                    f.write('class_date,class_time,class_url,processed\n')
            with open(filename, 'a') as f:
                try:
                    for day in days:
                        class_date = day.find('div', {'class':'class-list-day-header'}).text.strip().split('\n')[0].split(', ')[1]
                        sessions = day.findAll('div', {'class':'class-list-day-class '})
                        for session in sessions:
                            class_time = session.find('div', {'class':'time'}).text.strip().split('-')[0]
                            class_url = session.find('div', {'class':'book'}).find('a')['data-url']
                            data = ','.join([class_date, class_time, class_url, '\n'])
                            f.write(data)
                except:
                    pass
            time.sleep(3)
            data = pd.read_csv(filename)
            data = data.drop_duplicates()
            data.to_csv(filename, index=False)
        return filename

    def get_class_attendance(self, select_date):
        """
        Takes the date and saves all attendance across all classes for that date
        Args:
            select_date (dateObj): date of attendance required datetime.date() object
        Returns:
            saved_files (list): list of file names that were saved
        """
        saved_files = []
        string_date = select_date#.strftime('%Y-%m-%d')
        date_attendance_url = '{}?date={}'.format(self.config_urls['URLS']['CLASS_ATTENDANCE'], string_date)
        html = self.requests_session.get(date_attendance_url).text
        soup = bs4(html, 'lxml')
        # confirm correct date
        top_date = soup.find('div', {'class':'class-list-day-header'}).text.strip().split('\n')[0].split(', ')[1].strip()
        top_date_obj = datetime.datetime.strptime(top_date, "%d %B %Y")
        if top_date_obj == datetime.datetime.strptime(string_date, '%Y-%m-%d'):
            classes = soup.find('div', {'class':'class-list-day'}).findAll('div', {'class':'class-list-day-class'})
            for a_class in classes:
                class_url = a_class.find('div', {'class':'book'}).find('a')['data-url']
                class_id = class_url.split('/')[-1]
                data = self.requests_session.get(class_url).text
                class_soup = bs4(data, 'lxml')

                string_time = class_soup.findAll('div', {'class':'title-secondary'})[1].text.strip().replace(':','').split(' - ')[0]
                # save attendance to .html file
                save_file = '{}Attendance {} {}.html'.format(self.download_folder, string_date, string_time)
                with open(save_file, 'w', encoding='utf-8') as f:
                    f.write(class_id + '\n' + data)
                saved_files.append(save_file)
        return saved_files
                
    def download_class_html(self):
        data = pd.read_csv('boxchamp_attenance_urls.csv')
        urls = data['class_url'].tolist()
        for url in urls:
            filename = url.split('/')[-1] + '.html'
            html = self.requests_session.get(url).text
            with open('attendance2/{}'.format(filename), 'w') as f:
                f.write(html)
            time.sleep(5)
            
    
    def __latest_member_details__(self, member_type):
        '''
        Saves csv file of members ready to be imported into database
        Args:
            member_type (str): takes in 'active' or 'inactive'
        Return:
            save_csv_file (str): name of the file that was saved
        '''
        if member_type.lower() == 'active':
            export_url = self.config_urls['URLS']['ACTIVE_MEMBER_EXPORT']
            list_url = self.config_urls['URLS']['ACTIVE_MEMBER_LIST']  
        elif member_type.lower() == 'inactive': # still to complete below
            export_url = self.config_urls['URLS']['INACTIVE_MEMBER_EXPORT']
            list_url = self.config_urls['URLS']['INACTIVE_MEMBER_LIST']
        
        # downloads csv file of current active boxchamp users
        def scrape_info(export_url, list_url, member_type):
            # make sure file is empty of other 'Ballistix CrossFit Somerset West_users_' files
            delete_files_list = os.listdir(self.download_folder)
            delete_files = ['{}\\{}'.format(self.download_folder, file) for file in delete_files_list if 'Ballistix CrossFit Somerset West_users_' in file]
            for delete_file in delete_files:
                os.remove(delete_file)
            # download csv from boxchamp
            self.driver.get(export_url)
            # gets the ids associated with each member email
            self.driver.get(list_url)
            html = self.driver.page_source
            soup = bs4(html, 'lxml')
            table_rows = soup.find('tbody').findAll('tr')
            email_ids = {}
            for row in table_rows:
                items = row.findAll('td')
                member_id = items[0].find('input')['data-id']
                email = items[5].text
                email_ids[email] = member_id
            # find & select downloaded file
            files = os.listdir(self.download_folder)
            file = ['{}\\{}'.format(self.download_folder, file) for file in files if 'Ballistix CrossFit Somerset West_users_' in file][0]
            # read in the csv file and do data cleaning
            csv_df = pd.read_csv(file)
            # delete downloaded file
            os.remove(file)
            emails = csv_df['Email']
            member_ids = [email_ids[email] for email in emails]
            csv_df['member_boxchamp_id'] = member_ids
            member_since = []
            member_until = []
            for item in csv_df['Contract start and end dates']:
                if isinstance(item, str):
                    date_since, date_until = item.split(' / ')
                    date_since = datetime.datetime.strptime(date_since, '%Y-%m-%d')
                    date_until = datetime.datetime.strptime(date_until, '%Y-%m-%d')
                    member_since.append(date_since)
                    member_until.append(date_until)
                else:
                    member_since.append(datetime.datetime(1900, 1, 1, 0, 0))
                    member_until.append(datetime.datetime(1900, 1, 1, 0, 0))
            csv_df['member_since'] = member_since
            if member_type == 'active':
                csv_df['member_until'] = datetime.datetime(2100, 1, 1, 0, 0)
                csv_df['is_current'] = 'yes'
            elif member_type == 'inactive':
                csv_df['member_until'] = member_until
                csv_df['is_current'] = 'no'
            membership_type = []
            membership_period = []
            #return csv_df ################################################
            indexes = csv_df[csv_df['Package(s)'].isnull()].index
            for index in indexes:
                csv_df.at[index, 'Package(s)'] = ''
            for item in csv_df['Package(s)']:
                if item == '':
                    membership_type.append('Unknown')
                    membership_period.append('Unknown')
                elif 'Session Package' in item:
                    membership_type.append('Per Session')
                    membership_period.append('Unknown')
                else:
                    mtype, mperiod = item.lower().split(' - ')
                    if 'advanced barbell' in mtype or '4+' in mtype:
                        mtype = 'unlimited'
                    else:
                        mtype = mtype.split('crossfit ')[1]
                    if 'monthly' in mperiod:
                        mperiod = 'monthly'
                    else:
                        mperiod = mperiod.split('month')[0] + 'month'
                    membership_type.append(mtype)
                    membership_period.append(mperiod)
            csv_df['membership_type'] = membership_type
            csv_df['membership_period'] = membership_period
            indexes = csv_df[csv_df['Mobile'].isnull()].index
            for index in indexes:
                csv_df.at[index, 'Mobile'] = '000000000'
            csv_df['Mobile'] = '0' + csv_df['Mobile'].astype(str).str.replace('\.0','')
#             csv_df['member_id'] = [i+1 for i in range(len(csv_df))]
            clean_df = pd.DataFrame()
            clean_df[['member_first_name','member_last_name','member_email','member_phone','member_boxchamp_id','member_since','member_until','is_current','membership_type','membership_period']] = csv_df[['First Name','Last Name','Email','Mobile','member_boxchamp_id','member_since','member_until','is_current','membership_type','membership_period']]
            # saves csv file ready to be uploaded to database
            save_file = '{}\\Ballistix_formatted_for_database_members_{}.csv'.format(self.database_folder, member_type)
            clean_df.to_csv(save_file, index=False)
            return save_file
        
        return scrape_info(export_url, list_url, member_type)
    
    def __latest_lead_details__(self, lead_type):
        '''
        Saves csv file of members ready to be imported into database
        Args:
            lead_type (str): takes in 'active' or 'inactive'
        Return:
            save_csv_file (str): name of the file that was saved
        '''
        if lead_type.lower() == 'active':
            leads_list_url = self.config_urls['URLS']['ACTIVE_LEAD_LIST']
        elif lead_type.lower() == 'inactive': # still to complete below
            leads_list_url = self.config_urls['URLS']['INACTIVE_LEAD_LIST']

        self.driver.get(leads_list_url)
        html = self.driver.page_source
        soup = bs4(html, 'lxml')
        rows = soup.find('tbody').findAll('tr')
        big_list = []
        for i, row in enumerate(rows):
            cols = row.findAll('td')
            lead_id = i+1
            lead_first_name = cols[1].text.split(' ')[0].strip()
            lead_last_name = cols[1].text.split(' ', 1)[1].strip()
            lead_email = cols[2].text
            lead_phone = cols[3].text
            if lead_type.lower() == 'active':
                lead_still_lead = 'yes'
            else:
                lead_still_lead = 'no'
            lead_boxchamp_id = cols[0].find('input')['data-id']
            lead_since = datetime.datetime.strptime(cols[8].text.split(' at')[0], '%d %b') # currently sets year to 1900
            lead_until = datetime.datetime(1900, 1, 1)
            if lead_type.lower() == 'active':
                lead_outcome = 'pending'
            elif lead_type.lower() == 'inactive':
                lead_outcome = 'converted'
            big_list.append([
#                 lead_id, 
                lead_first_name, 
                lead_last_name, 
                lead_email, 
                lead_phone, 
                lead_still_lead, 
                lead_boxchamp_id, 
                lead_since, 
                lead_until, 
                lead_outcome])
        df = pd.DataFrame(big_list, columns=[
#             'lead_id',
            'lead_first_name',
            'lead_last_name',
            'lead_email',
            'lead_phone',
            'lead_still_lead',
            'lead_boxchamp_id',
            'lead_since',
            'lead_until',
            'lead_outcome'])
        save_file = '{}\\Ballistix_formatted_for_database_leads_{}.csv'.format(self.database_folder, lead_type.lower())
        df.to_csv(save_file, encoding='utf-8', index=False)
        return save_file
    
    def __parse_attendance_html__(self, html_file):
        '''
        Takes in each html file and parses to an output of:
            attended_member_boxchamp_ids: list of 'member_boxchamp_id' for each member attended
            attended_lead: list of dictionaries with 'lead_email' and 'lead_full_name' as keys. Outputs lowercase values
            attended_non_member: lsit of dictionaries with 'non_member_email' & 'non_member_full_name' as keys. Lowercase values.
        '''
        # check if file exists, if so, then don't parse
        json_save_file = html_file.replace('.html',' (parsed).json')
#         if json_save_file not in os.listdir(self.download_folder):    
        with open(self.download_folder + html_file) as f:
            html = f.read()
        soup = bs4(html, 'lxml')
        class_id = html.split('\n')[0]
#         class_id = html_file.split('.')[0]
        class_day_of_week, class_date = soup.findAll('div', {'class':'title-primary'})[1].text.split(', ')
        class_time = soup.findAll('div', {'class':'title-secondary'})[1].text.strip().split(' - ')[0].replace(':','')
        class_date = datetime.datetime.strptime(class_date, '%d %b %Y')
        class_year = class_date.strftime('%Y')
        class_month = class_date.strftime('%m').replace('0','')
        class_day = class_date.strftime('%d').replace('0','')
        class_date = class_year + ',' + class_month + ',' + class_day
        attendees = soup.find('ul', {'class':'row list editable'}).findAll('li', {'class':'taken'})
        attended_member_boxchamp_ids = []
        attended_leads = []
        attended_non_members = []
        for i in attendees:
            # scrapes member details
            try:
                member_boxchamp_id = i.find('a')['data-url']
                member_boxchamp_id = member_boxchamp_id.split('/')[-1].split('?')[0]
                attended_member_boxchamp_ids.append(member_boxchamp_id)
            # scrapes lead and non-member details
            except:
                time.sleep(3)
                data_url = i['data-url']
#                 self.driver.get(data_url)
                data_html = self.requests_session.get(data_url).text
                small_soup = bs4(data_html, 'lxml')
                try:
                    name_email = small_soup.find('div', {'class':'athlete-name'}).text.strip()
                    full_name, email = name_email.split(' (')
                    full_name = full_name.lower()
                    email = email.replace(')','').lower()
                    # if non-member
                    if email == 'a@a.com' or email == 'a@o.com':
                        attended_non_members.append({'non_member_email':email,'non_member_full_name':full_name})
                    # if lead
                    else:
                        attended_leads.append({'lead_email':email,'lead_full_name':full_name})
                except:
                    pass

        data = {'class_date':class_date, 'class_time':class_time, 'class_day':class_day_of_week, 'class_id':class_id, 'members':attended_member_boxchamp_ids, 'leads':attended_leads, 'non_members':attended_non_members}
        json_data = json.dumps(data)
        with open(self.download_folder + json_save_file, 'w') as f:
            f.write(json_data)

        return json_save_file
#         else:
#             return '"' + json_save_file + '"' + ' already exists and has been parsed'

    def run_active_members(self):
        '''
        Returns dataframe of active members in same format as used in Database
        '''
        self.driver.get(self.config_urls['URLS']['ALL_MEMBERS'])

        html = self.driver.page_source
        soup = bs4(html, 'lxml')
        table_rows = soup.find('tbody').findAll('tr', {'class':'clickable-row'})

        data = []
        for row in table_rows:
            cols = row.findAll('td')
            member_first_name = cols[3].text.strip()
            member_last_name = cols[4].text.strip()
            member_email = cols[5].text.strip()
            member_phone = ''
            member_boxchamp_id = int(cols[0].find('input')['data-id'])
            member_since = ''
            member_until = ''
            is_current = 'yes'
            membership_type_period = cols[6].text.strip()
            membership_type = membership_type_period
            if 'crossfit' in membership_type_period:
                membership_type = membership_type_period.split(' - ')[0].split('crossfit ')[1]
            else:
                membership_type = 'Per Session'
            if len(membership_type_period.split(' - ')) < 2:
                membership_period = 'Unknown'
            else:
                membership_period = membership_type_period.split(' - ')[1].split(' (')[0]
            data.append([member_first_name, member_last_name, member_email, member_phone, member_boxchamp_id, member_since, member_until, is_current, membership_type, membership_period])

            # append data into DataFrame
        columns=['member_first_name','member_last_name','member_email','member_phone','member_boxchamp_id','member_since','member_until','is_current','membership_type','membership_period']
        member_df = pd.DataFrame(data, columns=columns)    
        return member_df

    def run_pending_leads(self):
        self.driver.get(self.config_urls['URLS']['ACTIVE_LEAD'])
        html = self.driver.page_source
        soup = bs4(html, 'lxml')
        table_rows = soup.find('tbody').findAll('tr')
        lead_data = []
        for row in table_rows:
            cols = row.findAll('td')
            lead_first_name = cols[1].text.split(' ')[0]
            lead_last_name = ' '.join(cols[1].text.split(' ')[1:])
            lead_email = cols[2].text.strip()
            lead_phone = cols[3].text.strip()
            lead_still_lead = 'yes'
            lead_boxchamp_id = cols[0].find('input')['data-id']
            this_year = datetime.datetime.now().year
            lead_since = datetime.datetime.strptime(cols[8].text.split(' at')[0] + str(this_year), '%d %b%Y').date() # not correct year for all entries
            lead_until = datetime.date(2100, 1, 1)
            lead_outcome = 'pending'
            lead_data.append([lead_first_name, lead_last_name, lead_email, lead_phone, lead_still_lead, lead_boxchamp_id, lead_since, lead_until, lead_outcome])
        # append data into DataFrame
        columns=['lead_first_name','lead_last_name','lead_email','lead_phone','lead_still_lead','lead_boxchamp_id','lead_since','lead_until','lead_outcome']
        lead_df = pd.DataFrame(lead_data, columns=columns)
        lead_df['lead_boxchamp_id'] = lead_df['lead_boxchamp_id'].astype(int)
        return lead_df
    
    def get_member_phone(self, member_boxchamp_id):
        member_url = self.config_urls['URLS']['ACTIVE_LEAD'] + str(member_boxchamp_id)
        self.driver.get(member_url)
        html = self.driver.page_source
        try:
            member_phone = html.split('href="tel:')[1].split('"')[0]
        except:
            member_phone = '0000000000'
        return member_phone
    
    def run_parse_all_attendance_html(self, date):
        files = os.listdir(self.download_folder)
        html_attendance_files = [file for file in files if '.html' in file and date in file]
        json_files = []
        for file in html_attendance_files:
            json_file = self.__parse_attendance_html__(file)
        return json_files
        
    def run_latest_current_lead_details(self):
#         self.login()
        saved_filename = self.__latest_lead_details__('active')
#         self.close()
        return saved_filename
    
    def run_latest_non_current_lead_details(self):
#         self.login()
        saved_filename = self.__latest_lead_details__('inactive')
#         self.close()
        return saved_filename
        
    def run_latest_current_member_details(self):
#         self.login()
        saved_filename = self.__latest_member_details__('active')
#         self.close()
        return saved_filename
    
    def run_latest_non_current_member_details(self):
#         self.login()
        saved_filename = self.__latest_member_details__('inactive')
#         self.close()
        return saved_filename
    
    def run_class_attendance(self, select_date):
#         self.login()
        saved_files = self.get_class_attendance(select_date)
#         self.close()
        print ('Saved files: {}'.format(', '.join(saved_files)))
        return 'Saved files: {}'.format(', '.join(saved_files))
    
    def run_format_all_class_attendances(self):
        files = os.listdir(self.download_folder)

#############################################################################
#                              PURE FUNCTIONS                               #
#############################################################################

def format_attendance_for_database(json_file, member_map, coach_map, assistant_map, class_time_map, date_map, lead_map):
    '''
    Takes in each json file and parses to an output of:
        class_id: boxchamp's class_id
        members: list of 'member_boxchamp_id' for each member attended
        leads: list of dictionaries with 'lead_email' and 'lead_full_name' as keys. Outputs lowercase values
        non_members: lsit of dictionaries with 'non_member_email' & 'non_member_full_name' as keys. Lowercase values.
    '''
    if json_file != None:
        attendance = []  
        with open(json_file) as f:
            json_data = json.load(f) #.read() ##########################
        class_id = json_data['class_id']
        class_date = json_data['class_date'] 
        class_time = json_data['class_time']
        class_day = json_data['class_day']
        date_id = date_map[class_date]['date_id']
        class_time_id = class_time_map[class_time]['class_time_id']
        coach_id = coach_map[class_day][class_time]
        assist_coach_id = assistant_map[class_day][class_time]
        boxchamp_class_id = json_data['class_id']
        # format members
        members = json_data['members']
        for member in members:
            try:
                member_id = member_map[int(member)]['member_id']
                non_member_email = ''
                non_member_full_name = ''
                lead_id = 1
                attendance.append([member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id])
            except:
                print ('Errors with formatting following members for database (format_attendance_for_database())')
                print (member)
                print (json_file)
        # format non-members
        non_members = json_data['non_members']
        for non_member in non_members:
            member_id = 1
            non_member_email = non_member['non_member_email']
            non_member_full_name = non_member['non_member_full_name']
            lead_id = 1
            attendance.append([member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id])
        # format leads
        leads = json_data['leads']
        for lead in leads:
            try:
                member_id = 1
                non_member_email = ''
                non_member_full_name = ''
                lead_id = lead_map[lead['lead_email']]['lead_id']
                attendance.append([member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id])
            except:
                member_id = 1
                non_member_email = lead['lead_email']
                non_member_full_name = lead['lead_full_name']
                lead_id = 1
                attendance.append([member_id, non_member_email, non_member_full_name, lead_id, date_id, class_time_id, coach_id, assist_coach_id, boxchamp_class_id])

        return attendance
    
def format_boxchamp_attendance_urls(file):
    data = pd.read_csv(file)
    data = data.drop_duplicates()
    return data

def member_changes(bc_active_members_df, db_active_members_df, db_inactive_members_df):
    '''
    Takes df's of active members from Boxchamp and Database, along with inactive members in Database, 
    then used to determine 3 lists
    Return:
            db_update_to_active_member (list)
            db_update_to_inactive_member (list) 
            db_add_new_member (list)
    '''
    
    bc_active_ids = bc_active_members_df['member_boxchamp_id'].tolist()
    db_active_ids = db_active_members_df['member_boxchamp_id'].tolist()
    db_inactive_ids = db_inactive_members_df['member_boxchamp_id'].tolist()
    
    # separate those that are new members and those that are old members that need to be reactivated
    db_update_to_active_member = []
    db_add_new_member = []
    new_in_boxchamp = [bc_id for bc_id in bc_active_ids if bc_id not in db_active_ids]
    for member_boxchamp_id in new_in_boxchamp:
        if member_boxchamp_id in db_inactive_ids:
            db_update_to_active_member.append(member_boxchamp_id)
        else:
            db_add_new_member.append(member_boxchamp_id)
            
    db_update_to_inactive_member = []
    
    # determine thoe that need to be deactivated as a member in the database
    for db_active_id in db_active_ids:
        if db_active_id not in bc_active_ids:
            db_update_to_inactive_member.append(db_active_id)
    
    return db_update_to_active_member, db_update_to_inactive_member, db_add_new_member

def add_new_members_to_db(db, db_add_new_member, bc_active_members_df):
    '''
    Takes in DataBase object, list of member_boxchamp_id's along with dataframe of latest BoxChamp active members.
    Retrieves the member_phone, member_since & member_until fields.
    Saves the new members details to the database
    '''
    new_members_for_db = pd.DataFrame()
    boxchamp = BoxChamp()
    for member_boxchamp_id in db_add_new_member:
        data = bc_active_members_df[bc_active_members_df['member_boxchamp_id'] == member_boxchamp_id]
        index = data.index
        member_phone = boxchamp.get_member_phone(member_boxchamp_id)
        data.loc[index, 'member_phone'] = member_phone
        todays_date = datetime.datetime.now().date()
        data.loc[index, 'member_since'] = todays_date
        data.loc[index, 'member_until'] = datetime.date(2100, 1, 1)
        new_members_for_db = new_members_for_db.append(data, ignore_index=True)
    boxchamp.close()
    data_for_db = new_members_for_db.values.tolist()
    db.multiple_insert_member(data_for_db)

def member_update_to_active_db(db, db_update_to_active_member, bc_active_members_df):
    '''
    Takes in DataBase object, list of member_boxchamp_id's along with dataframe of latest BoxChamp active members.
    Retrieves the member_phone, member_since & member_until fields.
    Saves the members update details to the database
    '''
    active_updates_for_database = pd.DataFrame()
    boxchamp = BoxChamp()
    for member_boxchamp_id in db_update_to_active_member:
        data = bc_active_members_df[bc_active_members_df['member_boxchamp_id'] == member_boxchamp_id]
        index = data.index
        member_phone = boxchamp.get_member_phone(member_boxchamp_id)
        data.loc[index, 'member_phone'] = member_phone
        data.loc[index, 'member_until'] = datetime.date(2100, 1, 1)
        active_updates_for_database = active_updates_for_database.append(data, ignore_index=True)
    boxchamp.close()
    data_for_db = active_updates_for_database.values.tolist()
    db.multiple_member_update_to_active(data_for_db)

def member_update_to_inactive_db(db, db_update_to_inactive_member, db_active_members_df):
    '''
    Takes in DataBase object, list of member_boxchamp_id's along with dataframe of latest BoxChamp active members.
    Retrieves the member_phone, member_since & member_until fields.
    Saves the members update details to the database
    '''
    inactive_updates_for_database = pd.DataFrame()
    for member_boxchamp_id in db_update_to_inactive_member:
        data = db_active_members_df[db_active_members_df['member_boxchamp_id'] == member_boxchamp_id]
        index = data.index
        todays_date = datetime.datetime.now().date()
        data.loc[index, 'member_until'] = todays_date
        inactive_updates_for_database = inactive_updates_for_database.append(data, ignore_index=True)
    
    data_for_db = inactive_updates_for_database.values.tolist()
    db.multiple_member_update_to_inactive(data_for_db)

def members_not_synced(db_df, bc_df):
    '''
    Checks if the list of details in boxchamp and in the database are synched. Returns bool
    '''
    return db_df['member_boxchamp_id'].sort_values().tolist() != bc_df['member_boxchamp_id'].sort_values().tolist()


# dealing with leads
def leads_not_synced(db_df, bc_df):
    '''
    Checks if the list of pending leads in boxchamp and in the database are synched. Returns bool
    '''
    return db_df['lead_boxchamp_id'].sort_values().tolist() != bc_df['lead_boxchamp_id'].sort_values().tolist()

def lead_changes(bc_pending_leads_df, db_pending_leads_df):
    '''
    Takes df's of pending leads from Boxchamp and Database then used to determine 2 lists
    Return:
            db_update_to_converted_lead (list)
            db_add_new_lead (list)
    '''
    
    bc_pending_ids = bc_pending_leads_df['lead_boxchamp_id'].tolist()
    db_pending_ids = db_pending_leads_df['lead_boxchamp_id'].tolist()
    
    # determine new leads in boxchamp that need to be added to database
    db_add_new_lead = []
    for bc_pending_id in bc_pending_ids:
        if bc_pending_id not in db_pending_ids:
            db_add_new_lead.append(bc_pending_id)
    
    # determine those that need to be set as converted lead in the database
    db_update_to_converted_lead = []
    for db_pending_id in db_pending_ids:
        if db_pending_id not in bc_pending_ids:
            db_update_to_converted_lead.append(db_pending_id)
    
    return db_update_to_converted_lead, db_add_new_lead

def add_new_leads_to_db(db, db_add_new_lead, bc_pending_leads_df):
    '''
    Takes in database object, list of member_boxchamp_id's along with dataframe of latest BoxChamp pending leads.
    Saves the new leads details to the database
    '''
    db_add_new_lead_df = pd.DataFrame()
    for item in db_add_new_lead:
        db_add_new_lead_df = db_add_new_lead_df.append(bc_pending_leads_df[bc_pending_leads_df['lead_boxchamp_id'] == item], ignore_index=True)
        
    data_for_db = db_add_new_lead_df.values.tolist()
    db.multiple_insert_lead(data_for_db)

def lead_update_to_converted_in_db(db, db_update_to_converted_lead, db_pending_leads_df):
    '''
    Takes in database object, list of member_boxchamp_id's along with dataframe of latest database pending leads.
    Updates those leads to 'converted'. Also updates the is_still_lead & the lead_until date.
    '''
    db_update_to_converted_lead_df = pd.DataFrame()
    for item in db_update_to_converted_lead:
        df = db_pending_leads_df[db_pending_leads_df['lead_boxchamp_id'] == item]
        db_update_to_converted_lead_df = db_update_to_converted_lead_df.append(df)
    db_update_to_converted_lead_df['lead_still_lead'] = 'no'
    todays_date = datetime.datetime.now().date()
    db_update_to_converted_lead_df['lead_until'] = todays_date
    db_update_to_converted_lead_df['lead_outcome'] = 'converted' 

    data_for_db = db_update_to_converted_lead_df.values.tolist()
    db.multiple_lead_update_to_converted(data_for_db)
    
# if __name__ == "__main__":
#     pass