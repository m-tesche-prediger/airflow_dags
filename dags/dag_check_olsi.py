from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import pymsteams

# working directory
pwd = str(Path(__file__).parent.absolute())


# setup MS TEAMS object
WEBHOOK_URL = "https://predigerlicht.webhook.office.com/webhookb2/deb92327-8060-49cf-8a80-d83d4476b7bb@a49c30ec-fbcb-49ed-aa08-109e364b37fe/IncomingWebhook/8b19c09de52a484da331172ea680a2d9/ef820e5a-5e03-49c3-8bf7-6e8255d3eaca"

# create connectorcard object with the Microsoft Webhook URL
myTeamsMessage = pymsteams.connectorcard(WEBHOOK_URL)


##############################################################################
#
# functions
#
##############################################################################

def check_folder(hook, path, max_time):
    
    # get sftp folder content 
    folder_list = hook.describe_directory(path)
    folder = pd.DataFrame.from_dict(folder_list, orient='index')
    
    # variables
    files = ''
    warning = "No warning"
    
    if not folder.empty:
       
        # filter only files
        files = folder[(folder['type'] == 'file')]

        # create datetime column
        files['modify_2'] = pd.to_datetime(files['modify'].astype(str), format='%Y%m%d%H%M%S').dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')

        # get current time
        now = pd.to_datetime('now').tz_localize('UTC').tz_convert('Europe/Berlin')
        print(now)

        # check if file time is not expired
        files['warning'] = np.where(now > files['modify_2'] + pd.Timedelta(max_time, 's'), True, False)

        if (files['warning'].any()):
            warning = "Warning on Folder: {}, Max time expired: {}".format(path,max_time)
            
            # create message
            myTeamsMessage.color("#FF0000")
    
            # create the section
            myMessageSection = pymsteams.cardsection()
            
            # Facts are key value pairs displayed in a list.
            myMessageSection.addFact("Ordner", path)
            myMessageSection.addFact("Max Dauer [Sek]", max_time)
            
            # Section Text
            myMessageSection.text("Bitte überprüfe den OLSI-Dienst")
            
            myTeamsMessage.addSection(myMessageSection)
            
            # Add text to the message.
            myTeamsMessage.text("**OLSI Dienst crashed**")
            
            # send message to MS Teams
            myTeamsMessage.send()
#            myTeamsMessage.printme()

    return warning, files


def main():
    # connect to remote sftp server
    sftp_hook = SFTPHook(ftp_conn_id = "magento_sftp")

    # all folders to check [path, seconds]
    list_to_check = {'/data1/exchange/export/wawi/order/order': 2100,
                     '/data1/exchange/export/wawi/order/webdirect': 360,
                     '/data1/exchange/export/wawi/order/catalog': 3900,
                     '/data1/exchange/export/wawi/order/wishlist': 360,
                     }

    # iterate through all folders
    for path, time in list_to_check.items():
#        print(path, time)
        w, f = check_folder(sftp_hook, path, time)
#        print(w)
#        print(f)

    # disconnet from remote
    sftp_hook.close_conn()
    

##############################################################################
#
# airflow DAG
#
##############################################################################

dag_name = 'check_olsi'

default_args = {
        'owner'                 : 'm.tesche',
        'description'           : 'Check OLSI',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 2),
        'email': ['m.tesche@prediger.de', '1501da79.prediger.de@emea.teams.ms'],
        'email_on_failure'      : True,
        'email_on_retry'        : True,
#        'retries'               : 1,
#        'retry_delay'           : timedelta(minutes=5)
    } 

with DAG(dag_name, default_args=default_args, schedule_interval="*/5 7-20 * * *", catchup=False) as dag:
        
        check_olsi = PythonOperator(task_id='check_olsi',
                                            python_callable=main
                                            )       
        
#        send_email = EmailOperator(task_id='send_email',
#                    to='m.tesche@prediger.de',
#                    subject= dag_name + ': Customer features wurden erzeugt',
#                    html_content=""" <h3>See attached file</h3> """,
#                    files=[default_args.get('file_name')]
#                    )

        check_olsi