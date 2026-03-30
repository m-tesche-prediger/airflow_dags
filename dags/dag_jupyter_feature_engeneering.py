from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.email_operator import EmailOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime, timedelta
from pathlib import Path

# working directory
pwd = str(Path(__file__).parent.absolute())


##############################################################################
#
# Collect data
#
##############################################################################

# ams addresses
def get_adresses_ams():    
    sql_visible_products="""
    SELECT * FROM am.addresses_erp;
    """
    hook = MySqlHook(mysql_conn_id='ams_mysql',
                         schema='am')
    addresses = hook.get_pandas_df(sql_visible_products)

    addresses.to_csv(project_folder + '/' + run_folder + '/addresses.csv', index=False)
    
# ams debitor + kreditor
def get_debitor_ams():    
    sql_visible_products="""
    SELECT * FROM am.erp_kto;
    """
    hook = MySqlHook(mysql_conn_id='ams_mysql',
                         schema='am')
    debitor = hook.get_pandas_df(sql_visible_products)

    debitor.to_csv(project_folder + '/' + run_folder + '/debitoren.csv', index=False)
    
# ams addresses with newsletter
def get_newsletter_ams():    
    sql_visible_products="""
SELECT erp.erp_address_id
FROM am.addresses_erp erp
    JOIN am.contacts_addressesNewsletter nl
    ON erp.contacts_contact_id = nl.contacts_contact_id
WHERE nl.is_doi_mainlist_validated = 1
AND nl.email IS NOT NULL;
    """
    hook = MySqlHook(mysql_conn_id='ams_mysql',
                         schema='am')
    newsletter = hook.get_pandas_df(sql_visible_products)

    newsletter.to_csv(project_folder + '/' + run_folder + '/newsletter.csv', index=False)
    

##############################################################################
#
# airflow DAG
#
##############################################################################

dag_name = 'feature_engeneering'

# create project folder
project_folder = pwd + '/' + dag_name
Path(project_folder).mkdir(exist_ok=True)

# set run_folder
run_folder = date.today().strftime('%Y-%m-%d')

default_args = {
        'owner'                 : 'm.tesche',
        'description'           : 'Feature engeneering',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 2),
        'email': ['m.tesche@prediger.de', '1501da79.prediger.de@emea.teams.ms'],
        'email_on_failure'      : True,
        'email_on_retry'        : True,
#        'retries'               : 1,
#        'retry_delay'           : timedelta(minutes=5),
        'project_folder'        : project_folder,
        'run_folder'            : run_folder,
        'file_name'             : project_folder + '/' + run_folder + '/features_' + date.today().strftime('%Y-%m-%d') + '.csv'
    } 

with DAG(dag_name, default_args=default_args, schedule_interval="00 4 * * 1", catchup=False) as dag:
        
        commands = f"mkdir -p {project_folder}/{run_folder}"
        make_run_folder = BashOperator(task_id='make_run_folder',
                                       bash_command=commands)
        
        get_companies = SFTPOperator(task_id="get_companies",
            ssh_conn_id="ams_sftp",
            local_filepath=project_folder + '/' + run_folder + "/company.csv",
            remote_filepath="/var/www/vhosts/ams.prediger.de/htdocs/tools/pre-import/data/possible_company.csv",
            operation="get"
            )        

        get_adresses_ams = PythonOperator(task_id='get_adresses_ams',
                                          python_callable=get_adresses_ams)        
        
        get_debitor_ams = PythonOperator(task_id='get_debitor_ams',
                                          python_callable=get_debitor_ams)
        
        get_newsletter_ams = PythonOperator(task_id='get_newsletter_ams',
                                          python_callable=get_newsletter_ams)
        
        calculate_features = PapermillOperator(task_id='calculate_features',
                                                input_nb=project_folder + "/customer_feature_engeneering.ipynb",
                                                output_nb=project_folder + '/' + run_folder + "/customer_feature_engeneering_{{ execution_date }}.ipynb",
                                                parameters={"filename": default_args.get('file_name'),
                                                            "projectfolder": default_args.get('project_folder'),
                                                            "runfolder": default_args.get('run_folder')}
                                            )
        
        send_email = EmailOperator(task_id='send_email',
                    to='m.tesche@prediger.de',
                    subject= dag_name + ': Customer features wurden erzeugt',
                    html_content=""" <h3>See attached file</h3> """,
#                    files=[default_args.get('file_name')]
                    )
        
        commands = f"ls -trd {project_folder}/*/ | head -n -7 | xargs rm -Rf --"
        keep_only_7 = BashOperator(task_id='keep_only_7',
                                       bash_command=commands)

        make_run_folder >> [get_companies, get_adresses_ams, get_debitor_ams, get_newsletter_ams] >> calculate_features >> send_email >> keep_only_7