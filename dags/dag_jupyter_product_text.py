from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.papermill_operator import PapermillOperator
#from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.email_operator import EmailOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import requests
import json
import time

# working directory
pwd = str(Path(__file__).parent.absolute())

##############################################################################
#
# Collect data
#
##############################################################################

# pim products no text
def get_notext_pim():    
    sql_products_no_text="""
    SELECT * FROM pro 
WHERE proetykeyi = 214
AND prodels = 0
AND prokatkeyi = 95
AND NOT prokeyi IN (SELECT komprokeyi FROM kom WHERE komcnli IN (2,3,6,7) AND komrakkeyi = 72)
AND ((NOT probnrc LIKE 'c%' AND prokeyi NOT IN (SELECT ppzsubkeyi FROM ppz))
OR prokeyi IN (SELECT ppzprokeyi FROM ppz));
    """
    hook = PostgresHook(postgres_conn_id='pim_pgsql',
                         schema='portlight')
    products_notext = hook.get_pandas_df(sql_products_no_text)

    products_notext.to_csv(project_folder + '/' + run_folder + '/pim_products_notext.csv', index=False)
    
# magento visible products, no product text
def get_data_magento():    
    sql_visible_products="""
    SELECT sku FROM `catalog_product_flat_1` 
    WHERE sku NOT REGEXP '-' 
    AND visibility = 4
    AND description IS NULL;
    """
    hook = MySqlHook(mysql_conn_id='magento_mysql',
                         schema='prediger_live')
    visible_products = hook.get_pandas_df(sql_visible_products)

    visible_products.to_csv(project_folder + '/' + run_folder + '/visible_products_magento.csv', index=False)
    
# magento categories
def get_categories_magento():    
    sql_categories="""
    SELECT
	MIN(IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id)) AS product_id,
	MIN(IF(a.sku IS NULL, cpe.sku, a.sku)) AS config_sku,
	MIN(IF(a.name IS NULL, cpf.name, a.name)) AS config_name,
	MIN(a.parent_id),
	MIN(a.child_id),
	MIN(cpe.entity_id) AS simple_id,
	MIN(cpe.sku) AS simple_sku,
	MIN(cpf.name) AS simple_name,
	MIN(c.category_id),
	MIN(c.name),
	MIN(c.heading),
	MIN(c.path),
	MIN(c.level)
FROM
	catalog_product_entity cpe
LEFT JOIN
	catalog_product_flat_1 cpf ON (cpe.entity_id = cpf.entity_id)
LEFT JOIN
	catalog_product_relation cpr ON (cpr.parent_id = cpe.row_id)
LEFT JOIN
(
	SELECT
		e.entity_id,
		f.sku,
		f.name,
		r.parent_id,
		r.child_id
	FROM catalog_product_entity as e
	LEFT JOIN
		catalog_product_flat_1 f ON (e.entity_id = f.entity_id)
	LEFT JOIN
		catalog_product_relation r ON (r.parent_id = e.row_id)
	WHERE r.parent_id IS NOT NULL
) AS a ON (a.child_id = cpf.entity_id)
LEFT JOIN
(
	SELECT
		ccp.product_id,
		ccp.category_id,
		cpf.name,
		cpf.heading,
		cpf.path,
		cpf.level
	FROM catalog_category_product ccp
	LEFT JOIN
		catalog_category_flat_store_1 cpf ON (ccp.category_id = cpf.entity_id)
	WHERE
		cpf.level >= 4
	AND cpf.path REGEXP '3953|4158|3976'
) AS c ON (c.product_id = IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id))
AND cpe.sku NOT REGEXP '-'
GROUP BY cpe.sku
HAVING MIN(c.level);
    """
    hook = MySqlHook(mysql_conn_id='magento_mysql',
                         schema='prediger_live')
    categories = hook.get_pandas_df(sql_categories)

    categories.to_csv(project_folder + '/' + run_folder + '/categories_product_context.csv', index=False)
    
# magento categories grouped
def get_categories_grouped_magento():    
    sql_categories_grouped="""
SELECT
	MIN(IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id)) AS product_id,
	MIN(IF(a.sku IS NULL, cpe.sku, a.sku)) AS config_sku,
	MIN(IF(a.name IS NULL, cpf.name, a.name)) AS config_name,
	MIN(a.parent_id),
	MIN(a.child_id),
	MIN(cpe.entity_id) AS simple_id,
	MIN(cpe.sku) AS simple_sku,
	MIN(cpf.name) AS simple_name,
	MIN(cat),
	MIN(nam),
	MIN(head),
	MIN(pat),
	MIN(lev)
FROM
	catalog_product_entity cpe
LEFT JOIN
	catalog_product_flat_1 cpf ON (cpe.entity_id = cpf.entity_id)
LEFT JOIN
	catalog_product_relation cpr ON (cpr.parent_id = cpe.row_id)
LEFT JOIN
(
	SELECT
		e.entity_id,
		f.sku,
		f.name,
		r.parent_id,
		r.child_id
	FROM catalog_product_entity as e
	LEFT JOIN
		catalog_product_flat_1 f ON (e.entity_id = f.entity_id)
	LEFT JOIN
		catalog_product_relation r ON (r.parent_id = e.row_id)
	WHERE r.parent_id IS NOT NULL
) AS a ON (a.child_id = cpf.entity_id)
LEFT JOIN
(
	SELECT
		ccp.product_id,
		GROUP_CONCAT(ccp.category_id SEPARATOR '|') AS cat,
		GROUP_CONCAT(cpf.name SEPARATOR '|') AS nam,
		GROUP_CONCAT(cpf.heading SEPARATOR '|') AS head,
		GROUP_CONCAT(cpf.path SEPARATOR '|') AS pat,
		GROUP_CONCAT(cpf.level SEPARATOR '|') AS lev
	FROM catalog_category_product ccp
	LEFT JOIN
		catalog_category_flat_store_1 cpf ON (ccp.category_id = cpf.entity_id)
	WHERE
		cpf.level >= 1
	AND cpf.path REGEXP '3953|4158|3976'
    GROUP BY ccp.product_id
) AS c ON (c.product_id = IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id))
AND cpe.sku NOT REGEXP '-'
GROUP BY cpe.sku
HAVING MIN(lev);
    """
    hook = MySqlHook(mysql_conn_id='magento_mysql',
                         schema='prediger_live')
    categories_grouped = hook.get_pandas_df(sql_categories_grouped)

    categories_grouped.to_csv(project_folder + '/' + run_folder + '/categories.csv', index=False)

# magento categories rooms
def get_categories_rooms_magento():    
    sql_categories_rooms="""
SELECT
	MIN(IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id)) AS product_id,
	MIN(IF(a.sku IS NULL, cpe.sku, a.sku)) AS config_sku,
	MIN(IF(a.name IS NULL, cpf.name, a.name)) AS config_name,
	MIN(a.parent_id),
	MIN(a.child_id),
	MIN(cpe.entity_id) AS simple_id,
	MIN(cpe.sku) AS simple_sku,
	MIN(cpf.name) AS simple_name,
	MIN(cat),
	MIN(nam),
	MIN(head),
	MIN(pat),
	MIN(lev)
FROM
	catalog_product_entity cpe
LEFT JOIN
	catalog_product_flat_1 cpf ON (cpe.entity_id = cpf.entity_id)
LEFT JOIN
	catalog_product_relation cpr ON (cpr.parent_id = cpe.row_id)
LEFT JOIN
(
	SELECT
		e.entity_id,
		f.sku,
		f.name,
		r.parent_id,
		r.child_id
	FROM catalog_product_entity as e
	LEFT JOIN
		catalog_product_flat_1 f ON (e.entity_id = f.entity_id)
	LEFT JOIN
		catalog_product_relation r ON (r.parent_id = e.row_id)
	WHERE r.parent_id IS NOT NULL
) AS a ON (a.child_id = cpf.entity_id)
LEFT JOIN
(
	SELECT
		ccp.product_id,
		GROUP_CONCAT(ccp.category_id SEPARATOR '|') AS cat,
		GROUP_CONCAT(cpf.name SEPARATOR '|') AS nam,
		GROUP_CONCAT(cpf.heading SEPARATOR '|') AS head,
		GROUP_CONCAT(cpf.path SEPARATOR '|') AS pat,
		GROUP_CONCAT(cpf.level SEPARATOR '|') AS lev
	FROM catalog_category_product ccp
	LEFT JOIN
		catalog_category_flat_store_1 cpf ON (ccp.category_id = cpf.entity_id)
	WHERE
		cpf.level >= 1
	AND cpf.path REGEXP '3953|4158|3976'
    GROUP BY ccp.product_id
) AS c ON (c.product_id = IF(a.entity_id IS NULL, cpe.entity_id, a.entity_id))
AND cpe.sku NOT REGEXP '-'
GROUP BY cpe.sku
HAVING MIN(lev);
    """
    hook = MySqlHook(mysql_conn_id='magento_mysql',
                         schema='prediger_live')
    categories_rooms = hook.get_pandas_df(sql_categories_rooms)

    categories_rooms.to_csv(project_folder + '/' + run_folder + '/categories_room.csv', index=False)
    
##############################################################################
#
# API Axsemantics
#
##############################################################################

PROJECT_ID = '1bcba0e2-25de-4705-b902-ad0cf9633cc4'
LICENSE_HOLDER = 'customer-prediger'

# get session token from refresh token
def get_token():
    
    headers = { 'content-type': "application/json" }
    
    payload = {"refresh_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2lkbS5heC1zZW1hbnRpY3MuY29tLyIsImF1ZCI6InhvaDNJZWdoZWVzaGllNkt1TTlJdW1hZTRhZmFlZG9vIiwiZW1haWwiOiJtLnRlc2NoZUBwcmVkaWdlci5kZSIsImlhdCI6MTYxNDAwNzA4N30.r734jKM4RLBwCLtIV9RukMDYVA5Peom8jHjsGK7dJzA"}

    url = "https://api.ax-semantics.com/v1/token-exchange/"
    
    data = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    items = json.loads(data.text)
    
    return items["id_token"]

# create collectio
def create_collection(token, name):
    
    headers = {
        'authorization': "JWT " + token,
        'content-type': "application/json"
        }
    
    payload = {"name":name,
               "language":"de-DE",
               "training_id":PROJECT_ID,
               "license_holder":LICENSE_HOLDER
                }
    
    url = "https://api.ax-semantics.com/v3/collections/"
    
    data = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    items = json.loads(data.text)
    
    return items["id"]

# upload product data
def upload_document(token, collection_id, data):
    
    # replace nan with empty string
    data.replace(np.nan, '', regex=True, inplace=True)
    
    headers = {
    'authorization': "JWT " + token,
    'content-type': "application/json"
    }

    payload = {"uid": str(data.config_sku),
                "name": str(data.Produktname),
                "Produktname": str(data["Produktname"]),
                "PIM_ID": str(data["PIM_ID"]),
                "Produktnummer_PIM": str(data["Produktnummer PIM"]),
                "Gruppen_Produkt": str(data["Gruppen-Produkt"]),
                "Candela_des_Leuchtmittels": str(data["Candela des Leuchtmittels"]),
                "Ausführung": str(data["Ausführung"]),
                "Hersteller": str(data["Hersteller"]),
                "Hersteller_Familie": str(data["Hersteller_Familie"]),
                "Lichtfarbe": str(data["Lichtfarbe"]),
                "Durchmesser": str(data["Durchmesser"]),
                "Länge": str(data["Länge"]),
                "collection": str(data["collection"]),
                "Dimmbar": str(data["Dimmbar"]),
                "Schutzart": str(data["Schutzart"]),
                "CRI": str(data["CRI"]),
                "Energielabel": str(data["Energielabel"]),
                "Ausführung2": str(data["Ausführung2"]),
                "Fassung": str(data["Fassung"]),
                "Abstrahlwinkel": str(data["Abstrahlwinkel"]),
                "Maximale_Bestückung": str(data["Maximale Bestückung"]),
                "Lichtstrom": str(data["Lichtstrom"]),
                "Regelung": str(data["Regelung"]),
                "Schutzklasse": str(data["Schutzklasse"]),
                "Farbe___Oberfläche": str(data["Farbe / Oberfläche"]),
                "Einbaumaß_Höhe": str(data["Einbaumaß Höhe"]),
                "Tiefe": str(data["Tiefe"]),
                "Pendellänge": str(data["Pendellänge"]),
                "Baldachin_Durchmesser": str(data["Baldachin Durchmesser"]),
                "Baldachin_Höhe": str(data["Baldachin Höhe"]),
                "Leuchtenständer_Länge": str(data["Leuchtenständer Länge"]),
                "Leuchtenständer_Breite": str(data["Leuchtenständer Breite"]),
                "Ausladung": str(data["Ausladung"]),
                "Leuchtenkopf_Länge": str(data["Leuchtenkopf Länge"]),
                "Leuchtenkopf_Breit": str(data["Leuchtenkopf Breit"]),
                "Leuchtenständer_Höhe": str(data["Leuchtenständer Höhe"]),
                "Leuchtenständer_Durchmesser": str(data["Leuchtenständer Durchmesser"]),
                "Leuchtenschirm_Länge": str(data["Leuchtenschirm Länge"]),
                "Leuchtenschirm_Breite": str(data["Leuchtenschirm Breite"]),
                "Leuchtenkopf_Höhe": str(data["Leuchtenkopf Höhe"]),
                "Leuchtenkopf_Durchmesser": str(data["Leuchtenkopf Durchmesser"]),
                "Baldachin_Länge": str(data["Baldachin Länge"]),
                "Baldachin_Breite": str(data["Baldachin Breite"]),
                "Leuchtenschirm_Höhe": str(data["Leuchtenschirm Höhe"]),
                "Leuchtenschirm_Durchmesser": str(data["Leuchtenschirm Durchmesser"]),
                "Breite": str(data["Breite"]),
                "Höhe": str(data["Höhe"]),
                "Einbaumaß_Tiefe": str(data["Einbaumaß Tiefe"]),
                "Einbaumaß_Länge": str(data["Einbaumaß Länge"]),
                "Einbaumaß_Breite": str(data["Einbaumaß Breite"]),
                "Einbaumaß_Durchmesser": str(data["Einbaumaß Durchmesser"]),
                "Akkuleuchte": str(data["Akkuleuchte"]),
                "Farbe": str(data["Farbe"]),
                "Ausrichtbarkeit": str(data["Ausrichtbarkeit"]),
                "Gewicht": str(data["Gewicht"]),
                "Zertifizierung": str(data["Zertifizierung"]),
                "Leuchtmittel_inklusive": str(data["Leuchtmittel inklusive"]),
                "Leuchtenkörper_Farbe": str(data["Leuchtenkörper Farbe"]),
                "Leuchtenschirm_Farbe": str(data["Leuchtenschirm Farbe"]),
                "Anzahl_Leuchtmittel": str(data["Anzahl Leuchtmittel"]),
                "Designer": str(data["Designer"]),
                "Lichtcharakteristik": str(data["Lichtcharakteristik"]),
                "Bestückung_Leuchtmittel": str(data["Bestückung/Leuchtmittel"]),
                "Kabelfarbe": str(data["Kabelfarbe"]),
                "Farbe_Fuss": str(data["Farbe Fuss"]),
                "Betriebsgerät": str(data["Betriebsgerät"]),
                "Style": str(data["Style"]),
                "Hersteller_Serie": str(data["Hersteller_Serie"]),
                "created_at": str(data["created_at"]),
                "Artikelnummer_ERP": str(data["Artikelnummer ERP"]),
                "config_sku": str(data["config_sku"]),
                "Kategorien": str(data["Kategorien"]),
                "Produkt_Kategorie": str(data["Produkt_Kategorie"]),
                "Anwendungsbereich": str(data["Anwendungsbereich"]),
                "Unterscheidungsmerkmale": str(data["Unterscheidungsmerkmale"]),
                "Anzahl_Unterscheidungsmerkmale": str(data["Anzahl Unterscheidungsmerkmale"])
              }
    
    url = "https://api.ax-semantics.com/v3/collections/" + str(collection_id) + "/document/"

    data = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    items = json.loads(data.text)
    
    return items['id'], items['blob']['PIM_ID']

# create text from product data
def create_text(token, uid):
    
    headers = {
    'authorization': "JWT " + token
    }
    
    payload = "{}"

    url = "https://api.ax-semantics.com/v3/documents/" + str(uid) + "/generate-content/"
    
    data = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    items = json.loads(data.text)
    
#    return items

# download text (documents)
def get_document(token, collection_id, uid):
    
    status = 'generated'
    
    headers = {
    'authorization': "JWT " + token,
    'content-type': "application/json"
    }
    
    payload = "{}"

    url = "https://api.ax-semantics.com/v3/documents?uid=" + str(uid) + "&processing_state=" + status + "&collection=" + str(collection_id) 
    
    data = requests.request("GET", url, data=json.dumps(payload), headers=headers)
    items = json.loads(data.text)
    
    return items['results'][0]['content_html']

# get data from api
def get_text_from_api(filename, projectfolder, runfolder, pim_file):
    # load product data
    data = pd.read_csv(filename, sep=';', encoding='utf-8', low_memory=True, decimal=",")
    
    # create empty dataframe for all text
    text = pd.DataFrame()
    
    # get api token
    token = get_token()
    
    # create collection
    collection_id = create_collection(token, runfolder)
    
    # dict of skus and pim_id to find document in collection
    uids = {}
    
    # iterate over all data objects
    #for i in range(data.shape[0]):
    for i in range(50):
        # upload data
        document_id, pim_id = upload_document(token, collection_id, data.iloc[i])
#        print(document_id, pim_id)
#       uids.append(data.iloc[i].config_sku)
        uids[data.iloc[i].config_sku] = pim_id
        
        # create text
        create_text(token, document_id)
        
    # wait 15 min
    time.sleep(900)
    
    # iterate over all uids
    for uid, pim_id in uids.items():
        # append text to dataframe
        result = {'Produktnummer': pim_id,
                  'product_text': get_document(token, collection_id, uid).replace('\n', '')}
#        print(result)
        text = text.append(result, ignore_index=True)
    
    # add and transform columns
    text['Beschreibung(SHOPTEXT:Shop)'] = '$' + text['product_text'] + '$'
    text['Projekt'] = 'PIM'
    text['Projektvariante'] = 'default'
    
    # save to csv
    text.to_csv(pim_file,
            index=False,
            sep=';',
            decimal=',',
            encoding='utf8',
            header=['Projekt','Projektvariante','Produktnummer','Beschreibung(SHOPTEXT:Shop)'],
            columns=['Projekt','Projektvariante','Produktnummer','Beschreibung(SHOPTEXT:Shop)'])

##############################################################################
#
# airflow DAG
#
##############################################################################

dag_name = 'axsemantics_product_text'

# create project folder
project_folder = pwd + '/' + dag_name
Path(project_folder).mkdir(exist_ok=True)

# set run_folder
run_folder = date.today().strftime('%Y-%m-%d')

default_args = {
        'owner'                 : 'm.tesche',
        'description'           : 'Product text data for axsemantics',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 2),
        'email': ['m.tesche@prediger.de', '1501da79.prediger.de@emea.teams.ms'],
        'email_on_failure'      : True,
        'email_on_retry'        : True,
#        'retries'               : 1,
#        'retry_delay'           : timedelta(minutes=5),
        'project_folder'        : project_folder,
        'run_folder'            : run_folder,
        'file_name'             : project_folder + '/' + run_folder + '/axsemantics_' + date.today().strftime('%Y-%m-%d') + '.csv',
        'pim_file'              : project_folder + '/' + run_folder + '/pim-product_text_' + date.today().strftime('%Y-%m-%d') + '.csv'
    } 

with DAG(dag_name, default_args=default_args, schedule_interval="00 6 * * *", catchup=False) as dag:
        
        commands = f"mkdir -p {project_folder}/{run_folder}"
        make_run_folder = BashOperator(task_id='make_run_folder',
                                       bash_command=commands)

        get_products_notext_pim = PythonOperator(task_id='get_products_notext_pim',
                                          python_callable=get_notext_pim)        
        
        get_visibility_magento = PythonOperator(task_id='get_visibility_magento',
                                          python_callable=get_data_magento)
        
        get_categories_magento = PythonOperator(task_id='get_categories_magento',
                                          python_callable=get_categories_magento)

        get_categories_grouped_magento = PythonOperator(task_id='get_categories_grouped_magento',
                                          python_callable=get_categories_grouped_magento)
        
        get_categories_rooms_magento = PythonOperator(task_id='get_categories_rooms_magento',
                                          python_callable=get_categories_rooms_magento)
        
        get_products_pim = SFTPOperator(task_id="get_products_pim",
            ssh_conn_id="pim_sftp",
            local_filepath=project_folder + '/' + run_folder + "/pim_products.zip",
            remote_filepath="/opt/pfs/unaice/axsemantics_products.zip",
            operation="get"
            )
         
        aggregate_data = PapermillOperator(task_id='aggregate_data',
                                                input_nb=project_folder + "/axsemantics_product_collection.ipynb",
                                                output_nb=project_folder + '/' + run_folder + "/axsemantics_product_collection_{{ execution_date }}.ipynb",
                                                parameters={"filename": default_args.get('file_name'),
                                                            "projectfolder": default_args.get('project_folder'),
                                                            "runfolder": default_args.get('run_folder')}
                                            )
        
        get_text_from_api = PythonOperator(task_id='get_text_from_api',
                                          python_callable=get_text_from_api,
                                          op_kwargs={"filename": default_args.get('file_name'),
                                                     "projectfolder": default_args.get('project_folder'),
                                                     "runfolder": default_args.get('run_folder'),
                                                     "pim_file": default_args.get('pim_file')}
                                            )
        
        upload_text_to_pim = SFTPOperator(task_id="upload_text_to_pim",
            ssh_conn_id="pim_sftp",
            local_filepath=default_args.get('pim_file'),
            remote_filepath="/opt/pfs/hotfolders/produktdaten_shop/" + "axsemantics_product_text.csv",
            operation="put"
            )
        
        send_email = EmailOperator(task_id='send_email',
                    to='m.tesche@prediger.de',
                    subject= dag_name + ': Datei für Produkttexte wurde erzeugt',
                    html_content=""" <h3>See attached file</h3> """,
                    files=[default_args.get('file_name')]
                    )
        
        commands = f"ls -trd {project_folder}/*/ | head -n -7 | xargs rm -Rf --"
        keep_only_7_days = BashOperator(task_id='keep_only_7_days',
                                       bash_command=commands)

        make_run_folder >> [get_products_notext_pim, get_products_pim, get_visibility_magento, get_categories_magento, get_categories_grouped_magento, get_categories_rooms_magento] >> aggregate_data >> get_text_from_api >> upload_text_to_pim >> send_email >> keep_only_7_days