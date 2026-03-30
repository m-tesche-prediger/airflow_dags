#!/usr/bin/env python
# coding: utf-8

import os, sys
import pandas as pd
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import date, timedelta
from pathlib import Path

# working directory
pwd = str(Path(__file__).parent.absolute())

# Hello Analytics Reporting API V4.

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = pwd + '/GA-to-Python-871ca78a57a7.json'
VIEW_ID = '31364405'
#SHEET_ID = ''

# For the full list of dimensions & metrics, check https://developers.google.com/analytics/devguides/reporting/core/dimsmets
DIMENSIONS = ['ga:dimension4','ga:pagePath','ga:deviceCategory','ga:browser','ga:browserVersion','ga:contentGroup1','ga:medium','ga:source','ga:campaign']
METRICS = ['ga:pageviews']
SAMPLING = 'LARGE'
PAGESIZE = '90000'

# past dates 
# yesterday = timedelta(days = 1)
START = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
END = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'samplingLevel': SAMPLING,
          'pageSize': PAGESIZE,
          'dateRanges': [{'startDate': START, 'endDate': END}],
          'metrics': [{'expression':i} for i in METRICS],
          'dimensions': [{'name':j} for j in DIMENSIONS]
        }]
      }
  ).execute()


def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print(header + ': ' + dimension)

      for i, values in enumerate(dateRangeValues):
        print('Date range: ' + str(i))
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print(metricHeader.get('name') + ': ' + value)

        
def convert_to_dataframe(response):
    
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
    finalRows = []
    

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      metrics = row.get('metrics', [])[0].get('values', {})
      rowObject = {}

      for header, dimension in zip(dimensionHeaders, dimensions):
        rowObject[header] = dimension
        
        
      for metricHeader, metric in zip(metricHeaders, metrics):
        rowObject[metricHeader] = metric

      finalRows.append(rowObject)
      
      
  api_data = pd.DataFrame(finalRows)

  return api_data


def process_data(data):
    
  # rename colums
  data = data.rename(columns={'ga:pagePath': 'url_with_param',
                              'ga:deviceCategory': 'screen',
                              'ga:browser': 'browser',
                              'ga:contentGroup1': 'page_type',
                              'ga:browserVersion': 'browser_version',
                              'ga:medium': 'medium',
                              'ga:source': 'source',
                              'ga:campaign': 'campaign'})

  # drop rows when ga:dimension4 has wrong format
  false_dimension_data = data[(data['ga:dimension4'].str.count('#') < 8)].index
  data.drop(false_dimension_data, inplace = True)    

  # create new columns with split values
  new = data["ga:dimension4"].str.split("#", n = 8, expand = True) 
  data['raw_cache_id'] = new[0]
  data['cachable'] = new[1]
  data['cache'] = new[2]
  data['ttl'] = new[3]
  data['user_time'] = new[4]
  data['cache_age'] = new[5]
  data['cached_by'] = new[6]
  data['url_param'] = new[7]
  data['browser_load_time'] = new[8]

  # split value into two parts
  split_cache_id = data['raw_cache_id'].str.split(" ", n = 1, expand = True)
  data['current_cache_id'] = split_cache_id[0]
  data['parent_cache_id'] = split_cache_id[1]
    
  # convert to dtypes
  data['browser_load_time'] = pd.to_numeric(data['browser_load_time'])
  data['cache_age'] = pd.to_numeric(data['cache_age'])
  data['ttl'] = pd.to_numeric(data['ttl'])
  data['user_time'] = pd.to_datetime(data['user_time'].astype(str), format='%Y-%m-%d %H:%M:%S')
    
  # if ttl = 0 then set cache to "NO CACHING"
  data['cache'] =  np.where(data['ttl'] == 0, 'NO CACHING', data['cache'])

  # drop columns
  data.drop(columns =["ga:dimension4"], inplace = True)
  data.drop(columns =["ga:pageviews"], inplace = True)
  data.drop(columns =["raw_cache_id"], inplace = True)

  # drop rows with negative browser_load_times
  false_browser_timing = data[(data['browser_load_time'] < 0)].index
  data.drop(false_browser_timing, inplace = True)

  # remove rows where user_time not in date_range
  end = pd.to_datetime(END) + pd.DateOffset(days=1)
  false_user_timing = data[(data['user_time'] < START) | (data['user_time'] > end)].index
  data.drop(false_user_timing, inplace = True)

  # remove url_param from url
  split = data['url_with_param'].str.rsplit("?", n=1, expand=True)
  data['url'] = split[0]
    
  # timedelta for cached_at
  data['cached_at'] = data['user_time'] - pd.to_timedelta(data['cache_age'], unit='s')

  return data


def sample_data(data, period):

  hit = data[(data['cache'] == 'HIT')].resample(period, on='user_time').agg({'cache': 'count'})
  hit = hit.rename(columns={'cache': 'hit'})

  miss = data[(data['cache'] == 'MISS')].resample(period, on='user_time').agg({'cache': 'count'})
  miss = miss.rename(columns={'cache': 'miss'})

  no_caching = data[(data['cache'] == 'NO CACHING')].resample(period, on='user_time').agg({'cache': 'count'})
  no_caching = no_caching.rename(columns={'cache': 'no_caching'})

  user = data[(data['cached_by'] == 'User')].resample(period, on='user_time').agg({'cached_by': 'count'})
  user = user.rename(columns={'cached_by': 'cached_by_user'})

  cache_warmer = data[(data['cached_by'] == 'cacheWarmer')].resample(period, on='user_time').agg({'cached_by': 'count'})
  cache_warmer = cache_warmer.rename(columns={'cached_by': 'cached_by_cacheWarmer'})
    
  cache_amasty = data[(data['cached_by'] == 'AMASTY')].resample(period, on='user_time').agg({'cached_by': 'count'})
  cache_amasty = cache_amasty.rename(columns={'cached_by': 'cached_by_amasty'})
    
  #hour.head(30)
  sample = pd.concat([hit, miss, no_caching, user, cache_warmer, cache_amasty], axis=1)
  sample.fillna(0, inplace=True)
  sample['percentage_user_warmer'] = (sample['cached_by_cacheWarmer'] + sample['cached_by_amasty']) / (sample['cached_by_cacheWarmer'] + sample['cached_by_amasty'] + sample['cached_by_user'])*100
  sample['percentage_uncached'] = sample['miss']/(sample['hit'] + sample['miss'])*100

  return sample


def save_daily_csv(data, filename):
  data.to_csv(filename, index=False) # reports_daily


def export_to_sheets(df):
    gc = pygsheets.authorize(service_file='client_secrets.json')
    sht = gc.open_by_key(SHEET_ID)
    wks = sht.worksheet_by_title('Sheet1')
    wks.set_dataframe(df,'A1')


def plot_save_report(sample, img):
    sample_gfx = sample[['hit', 'miss', 'no_caching', 'cached_by_user', 'cached_by_cacheWarmer', 'cached_by_amasty', 'percentage_user_warmer', 'percentage_uncached']]
    sample_gfx.index = sample_gfx.index.strftime('%H:%M:%S')
    filename_png = 'caching_report_' + END + '.png'

    with sns.axes_style("white"):
        sns.set_style("ticks")
        sns.set_context("talk")
        sns.set(rc={'figure.figsize':(30,15)})

        #Plot graph with 2 y axes
        fig, ax1 = plt.subplots()

        # plot details
        bar_width = 0.85
        epsilon = .015
        line_width = 1
        opacity = 0.7
        pos_bar_positions = np.arange(len(sample_gfx['hit'])) * 3.5
        neg_bar_positions = pos_bar_positions + bar_width * 1.3

        # make bar plots
        hpv_pos_mut_bar = plt.bar(pos_bar_positions, sample_gfx['hit'], bar_width,
                                  color='blue',
                                  edgecolor='blue',
                                  label='HIT')


        hpv_pos_cna_bar = plt.bar(pos_bar_positions, sample_gfx['miss'], bar_width,
                                  bottom=sample_gfx['hit'],
                                  alpha=opacity,
                                  color='red',
                                  edgecolor='red',
                                  linewidth=line_width,
                                  label='MISS')

        hpv_pos_both_bar = plt.bar(pos_bar_positions, sample_gfx['no_caching'], bar_width,
                                   bottom=sample_gfx['hit'] + sample_gfx['miss'],
                                   alpha=opacity,
                                   color='grey',
                                   edgecolor='grey',
                                   linewidth=line_width,
                                   hatch='0',
                                   label='NO CACHING')

        hpv_neg_cna_bar = plt.bar(neg_bar_positions, sample_gfx['cached_by_cacheWarmer'], bar_width-epsilon,
                                  alpha=0.7,
                                  color="green",
                                  hatch='0',
                                  edgecolor='green',
                                  ecolor="green",
                                  linewidth=line_width,
                                  label='cacheWarmer')

        hpv_neg_mut_bar1 = plt.bar(neg_bar_positions, sample_gfx['cached_by_amasty'], bar_width,
                                  bottom = sample_gfx['cached_by_cacheWarmer'],
                                  alpha=0.2,
                                  color='green',
                                  edgecolor='green',
                                  ecolor="green",
                                  linewidth=line_width,
                                  hatch='//',
                                  label='Amasty')

        hpv_neg_mut_bar = plt.bar(neg_bar_positions, sample_gfx['cached_by_user'], bar_width,
                                  bottom = sample_gfx['cached_by_cacheWarmer'] + sample_gfx['cached_by_amasty'],
                                  alpha=0.6,
                                  color='orange',
                                  edgecolor='orange',
                                  ecolor="orange",
                                  linewidth=line_width,
                                  hatch='0',
                                  label='USER')

        """
        hpv_neg_both_bar = plt.bar(neg_bar_positions, neg_both_pcts, bar_width-epsilon,
                                   bottom=neg_cna_pcts+neg_mut_pcts,
                                   color="white",
                                   hatch='0',
                                   edgecolor='#0000DD',
                                   ecolor="#0000DD",
                                   linewidth=line_width,
                                   label='HPV- Both')
        """

        plt.title('Caching-Report vom ' + END + ', HIT, MISS und NO CACHING - downsample auf 15 Minuten', pad=15)
        plt.xticks(neg_bar_positions, sample_gfx.index , rotation=90)
        plt.ylabel('Seitenaufrufe')
        plt.legend(loc='best')

        #Set up ax2 to be the second y axis with x shared
        ax2 = ax1.twinx()
        #Plot a line    
        ax2.plot(neg_bar_positions, sample_gfx[['percentage_uncached']], color = 'red', linestyle='-.', linewidth=2)
        ax2.set_ylabel('% uncached', color='red')
        [tl.set_color('red') for tl in ax2.get_yticklabels()]

        #Set up ax2 to be the second y axis with x shared
        ax3 = ax1.twinx()
        #Plot a line    
        ax3.plot(neg_bar_positions, sample_gfx[['percentage_user_warmer']], color = 'green', linestyle=':', linewidth=2)
        ax3.set_ylabel('% cache warming', color='green')
        [tl.set_color('g') for tl in ax3.get_yticklabels()]

        #sns.despine()

        fig.savefig(img) 


def get_probabilities(data):
    f = data.copy()

    # total pageviews
    total_pageviews = data['url_with_param'].count()
    
    # sort by url
    f = f.sort_values(['url_with_param', 'parent_cache_id'], ascending=[True, True])

    # make tmp columns
    f['match'] = f.parent_cache_id.eq(f.parent_cache_id.shift())
    f['transition'] = f.url_with_param.eq(f.url_with_param.shift())

    # invert true and false
    f['num_invalidations'] = np.invert(f['match'])
    f['transition_2'] = np.invert(f['transition'])

    # transform true and false to 1 and 0 to sum up
    f['num_invalidations'] = f['num_invalidations'].astype(int)
    f['transition_2'] = f['transition_2'].astype(int)

    # drop tmp columns
    f = f.drop(['match'], axis=1)
    f = f.drop(['transition'], axis=1)

    # drop rows where transistion to different url takes place
    f.drop(f[(f.transition_2 == 1) & (f.num_invalidations == 1)].index, inplace=True)
    
    # group by url
    e = f.groupby(['url_with_param']).agg({'url': len, 'num_invalidations': sum})

    #e['num_correct'] = e.apply(lambda x: x['num_invalidations']/2, axis=1).round(0)
    #e = e.sort_values(by='num_invalidations', ascending=False)

    e = e.rename(columns={'url': 'num_pageviews'})

    e['probability_uncached'] = (e['num_invalidations'] / e['num_pageviews']) * (e['num_pageviews'] / total_pageviews) / (((e['num_invalidations'] / e['num_pageviews']) * (e['num_pageviews'] / total_pageviews)+((e['num_pageviews'] - e['num_invalidations']) / e['num_pageviews'] * e['num_pageviews'] / total_pageviews)))

    e.sort_values('num_invalidations', ascending=False)
    
    e.to_csv(pwd + '/' + END + 'probabilities.csv')


def plot_save_warming_load(data, period):
    warming_load = data[{'url_with_param', 'cached_at', 'cached_by'}].copy()
    warming_load.set_index('url_with_param', inplace=True)
    warming_load.drop_duplicates(keep=False, inplace=True)

    wl= warming_load.resample(period, on='cached_at').agg('count')
    #wll= warming_load.resample(period, on='cached_at')
    #wl = wll.agg({'url_with_param': 'count'})

    filename_load = 'caching_load_' + END + '.png'

    #wl['url_with_param'].plot.line(figsize=(25,15), color = 'red', rot=0, title='Warmer load per minute').get_figure().savefig(pwd + '/reports_daily/' + filename_load)
    wl.plot.line(figsize=(25,15), color = 'red', rot=0, title='Warmer load per minute').get_figure().savefig(pwd + '/' + filename_load)


"""
def main():
  # Initalize Google API
  analytics = initialize_analyticsreporting()
  # Get Report
  response = get_report(analytics)
  # Transform report to dataframe
  raw_data = convert_to_dataframe(response)
  # Process dataframe
  data = process_data(raw_data)
  # Save data
  save_daily_csv(data)
  # Save probabilities of getting uncached page
  get_probabilities(data)
  # Sample data (15min) for visualization
  sample = sample_data(data, '15min')
  # Plot and save report as PNG
  plot_save_report(sample)
  # Calculate, plot and save warming load
  plot_save_warming_load(data, '1min')
  # send report via email
  send_report(END)
    
  #export_to_sheets(data)   
  #print(response)
  #print_response(response)
  #print_onscreen(data)

if __name__ == '__main__':
  main()
"""

# Refernzen:
# 
# * [gaExport](https://github.com/RitwikGA/GoogleAnalytics-Pandas-Sheet/blob/master/gaExport.py)
# * [matplotlib - stacked barplot](https://pstblog.com/2016/10/04/stacked-charts)


#################################################################
# airflow DAG
#################################################################

# combine functions for operators in DAG

def load_data(filename):
    # Initalize Google API
    analytics = initialize_analyticsreporting()
    # Get Report
    response = get_report(analytics)
    # Transform report to dataframe
    raw_data = convert_to_dataframe(response)
    # Process dataframe
    data = process_data(raw_data)
    # Save data
    save_daily_csv(data, filename)

def visualize(filename, img):
    # get data from file
    data = pd.read_csv(filename)
    data['user_time'] = pd.to_datetime(data['user_time'].astype(str), format='%Y-%m-%d %H:%M:%S')
    data['cached_at'] = pd.to_datetime(data['cached_at'].astype(str), format='%Y-%m-%d %H:%M:%S')
    # Sample data (15min) for visualization
    sample = sample_data(data, '15min')
    # Plot and save report as PNG
    plot_save_report(sample, img)


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

dag_name = 'cacheWarmingReport'

# create project folder
project_folder = pwd + '/' + dag_name
Path(project_folder).mkdir(exist_ok=True)

# yesterday
yesterday = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

default_args = {
        'owner'                 : 'm.tesche',
        'description'           : 'Cache Warming Report',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 12, 1),
        'email': ['m.tesche@prediger.de', '1501da79.prediger.de@emea.teams.ms'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=2),
        'file_name'             : project_folder + '/' + yesterday + '-caching-report_raw_data.csv',
        'file_name_png'         : project_folder + '/' + yesterday + '-caching-report_graph.png'
} 

with DAG(dag_name, default_args=default_args, schedule_interval="30 6 * * *", catchup=False) as dag:
        
        load_data_from_api = PythonOperator(task_id='load_data_from_api',
                                            python_callable=load_data,
                                            op_kwargs={'filename': default_args.get('file_name')}
                                            )

        visualize_data = PythonOperator(task_id='visualize_data',
                                            python_callable=visualize,
                                            op_kwargs={'filename': default_args.get('file_name'),
                                                       'img': default_args.get('file_name_png')
                                                       }
                                            )
                      
        send_email = EmailOperator(task_id='send_email',
                    to='m.tesche@prediger.de, 1501da79.prediger.de@emea.teams.ms',
                    subject= dag_name + ': daily report',
                    html_content="""
<p>Short explanantion for GRAPH:</p>
<ul>
<li>data is downsampled into 15 min segments</li>
<li>left bar shows all occured pageviews devided into 3 segments:
<ul>
<li>blue: HIT</li>
<li>red: MISS</li>
<li>grey: NO CACHING</li>
</ul>
</li>
<li>right bar shows all pageviews devided into 3 segments taken by user-agent that warmed this url. This is a measurement of the performance for the two cache warmer:
<ul>
<li>green: internal cache warmer</li>
<li>light green dashed: Amasty Extension</li>
<li>orange: User</li>
</ul>
</li>
<li>red dotted line shows the percentage of uncached pageviews</li>
<li>green dotted line shows the percentage of cached by CacheWarmer (any of the two) and user</li>
</ul>
<p>In short: the more blue and green colors you see the better. Red line should be between 5-10%. The green line should reach up to 50-70% for good cache warmer performance.</p>
<blockquote>
<p>Keep attention when you see a strong rise in the red line and at the same time a decrease in pageviews with user-agents from cache warmers depicted in the right bar (smaller green parts). This is a strong indication that the entire cache has been flushed.</p>
</blockquote>
                    """,
                    files=[default_args.get('file_name_png')]
                    )
        
        commands = f"find {project_folder}/ -mindepth 1 -mtime +7 -delete"
        keep_only_7_days = BashOperator(task_id='keep_only_7_days',
                                       bash_command=commands)

        load_data_from_api >> visualize_data >> send_email >> keep_only_7_days