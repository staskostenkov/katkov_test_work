#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Тестовое задание:
# Сделать простой cashflow отчёт на основании данных полученных через Plaid из банка Bank of Amerika 
# (использовать тестовые реквизиты от самого plaid). Для получения данных из банка использовать сервис 
# https://plaid.com/eu/ (он бесплатен для тестирования).

# Пример “простого cashflow” (это разбвки income/expenses по категориям и по месяцам): 
# https://www.dropbox.com/s/lywnj7f1fchukkg/Screenshot%202020-05-24%2013.43.08.png?dl=0

# В качестве результата выполнения задания нужно предоставить:
# 1) ссылка на документ в google spreadsheets с 2 листами:
#   - лист с “сырыми данными” по транзакциям из plaid. 
#   - лист с cashflow отчётам, который построен по аналогии с примером со скриншота.
# 2) ссылку на репозиторий с кодом, который был использован для получения данных через plaid и 
# сохранения в google spreadsheets

# Результаты выполнения нужно отправить через эту форму: https://airtable.com/shra9dmxAXr5D67Lu

# Срок выполнения: результаты выполнения задания нужно отправить не позднее 4 июня

# Комментарии к заданию:
# - Нам важно понять насколько ты владеешь python для решения базовых задач по извлечению, сохранению и 
# обработке данных из сторонних систем.
# - Если не сможешь выполнить всё задание, но выполнишь его часть, то всё равно можешь прислать результаты. 
# Задание построено так, что имеет несколько частей, которые различаются по сложности. Поэтому, выполнив 
# даже часть задания, ты покажешь то, что умеешь. Используй поле “Комментарии к результату” в форме для того, 
# чтобы дать пояснения о том, что удалось сделать, а что нет.
# - Задавать вопросы и уточнять задание можно в этом чате.


# # Request & data loading

# In[4]:


# plaid.com/docs/retrieve-transactions-request
import requests
import json
import pandas as pd


PLAID_CLIENT_ID='5f01a56a00f5020011f9c2fd'
PLAID_SECRET='49eedfa156db01d6674fc99b2f9fb0'
PLAID_PUBLIC_KEY='9908c5c8242c1da1e39a2f8f0ca758'
PLAID_PRODUCTS='transactions'
PLAID_COUNTRY_CODES='US'

url1 = 'https://sandbox.plaid.com/transactions/get'
payload = {'client_id':PLAID_CLIENT_ID,
        'secret':PLAID_SECRET,
        "access_token":"access-sandbox-5f8a3c5a-38e9-4283-abca-6dea9d11695c",
        #'institution_id': 'ins_1'
        #'institution_name': 'Bank of America',
        'start_date':'2001-01-01',
        'end_date': '2020-06-01',
        #'count': 250,
        #'offset': 0,
                         
                        }
headers = {'Content-Type': 'application/json',}
request = requests.post(url1, 
                   json={"key": "value"}, 
                   auth=('user_good', 'pass_good'),
                   data=json.dumps(payload), headers=headers)
print('request.status_code', request.status_code)

df = pd.DataFrame(pd.json_normalize(json.loads(request.text)['transactions']))
#df[:5]


# # Data Preproccesing

# In[5]:


def date_prep(df1):
    """
    take features from a date
    """
    data = df1.copy()
    dt_col = 'date'
    data[dt_col] = pd.to_datetime(data[dt_col])
    attrs = [
        "year",
        "month",
    ]
    
    month_str = {'1': 'Yan',
        '2': 'Feb',
        '3': 'Mar',
        '4': 'Apr',
        '5': 'May',
        '6': 'Jun',
        '7': 'Jul',
        '8': 'Auv',
        '9': 'Sep',
        '10': 'Okt',
        '11': 'Nov',
        '12': 'Dec'
    }
    for attr in attrs:
        data[attr] = getattr(data[dt_col].dt, attr).astype(int)
    data = data.sort_values(by=['year', 'month'])
    data['month_str'] = data['month'].astype(str)
    data['month_str'] = data['month_str'].map(month_str)
    data['year_str'] = data['year'].astype(str)
    data['date'] = data['month_str'] + ' ' + data['year_str']
    return data

def prep_category(df1):
    """
    some preproccesing features
    """
    df = df1.copy()
    df['category'] = df['category'].str.join(',')    
    df['flow_direct'] = 'Expense'
    df['flow_direct'][df.amount<0] = 'Income'
    df['amount'] = df['amount']*(-1)
    df['merchant_name'] = df['merchant_name'].fillna('Missing_value')
    return df


def create_total(df1):
    """
    create addition rows - total values for category & merchant_name
    and then takes them in pivot table
    """
    df = df1.copy()
    zz = pd.DataFrame(df1.groupby(["category",'date'])['amount'].sum())
    zz['merchant_name'] = 'total'
    zz['flow_direct'] = 'Expense'
    zz['category']=''
    zz['date']=''
    for ii in range(len(zz)):
        zz['category'].iloc[ii] = zz.index[ii][0]
        zz['date'].iloc[ii] = zz.index[ii][1]
        if zz.category.iloc[ii] in df1.category[df1.flow_direct=='Income'].unique():
            zz['flow_direct'].iloc[ii] = 'Income'

    czz = pd.DataFrame(df1.groupby(["flow_direct",'date'])['amount'].sum())
    czz['category'] = 'total'
    czz['merchant_name'] = ''
    czz['flow_direct'] = ''
    czz['date']=''
    for ii in range(len(czz)):
        czz['flow_direct'].iloc[ii] = czz.index[ii][0]
        czz['date'].iloc[ii] = czz.index[ii][1]
    df2 = pd.concat([df1, zz, czz], ignore_index=True)
    return df2

# create dataframe after ETL
df1 = date_prep(prep_category(df))[['flow_direct', 'category', 
                                    'merchant_name',  'date',  'amount', ]] # 'year', 'month',
#df1[:3]

# create total rows for categories
df2 = create_total(df1)
# create output pivot table
qqq = df2.pivot_table('amount', ['flow_direct', 'category', 'merchant_name'], 'date', dropna=True, 
                      aggfunc='sum', margins_name='Grand total', margins=True)
qqq= qqq.fillna(0)

# transform multi-index pivot table to dataframe columns
qqq['expense/income'] = ''
qqq['category1'] = ''
qqq['category2'] = ''

for ii in range(len(qqq.index)):
    qqq['expense/income'].iloc[ii] = qqq.index[ii][0]
    qqq['category1'].iloc[ii] = qqq.index[ii][1]
    qqq['category2'].iloc[ii] = qqq.index[ii][2]
flow_list = qqq['expense/income'].unique()
cat1_list = qqq['category1'].unique()
cat2_list = qqq['category2'].unique()

def trans_qqq(qqq, col, col_list, qqq_index):
    ilist=0
    for ii in range(len(qqq_index)):
        if qqq[col].iloc[ii]=='total':
            ii +=1
        else:
            if (qqq[col].iloc[ii]==col_list[ilist]):
                ilist+=1
            else:
                qqq[col].iloc[ii]=''
    return qqq[col]

qqq['expense/income'] = trans_qqq(qqq, 'expense/income', flow_list, qqq.index)
qqq['category1'] = trans_qqq(qqq, 'category1', cat1_list, qqq.index)

# create structure for output dataframe
outdataframe = pd.DataFrame(columns = ['expense/income', 'category1', 'category2', 
                                      'Nov 2019','Dec 2019','Yan 2020', 
                                      'Feb 2020','Mar 2020','Apr 2020',
                                       'May 2020','Grand total'])
outdataframe = pd.concat([outdataframe, qqq], ignore_index=True)
#outdataframe


# In[6]:


# data uploading to google spreadsheet
# link for googlespreadsheet file is 
# https://docs.google.com/spreadsheets/d/1-3K9usjWgwA8wcMxtPZcH4xyJ-psQZ3-meR5pau58ZI/edit?usp=sharing
from df2gspread import df2gspread as d2g
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import httplib2


CREDENTIALS_FILE = 'katkov-test-976d3a85df31.json'  #  ← имя скаченного файла с закрытым ключом
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, 
                                                               ['https://www.googleapis.com/auth/spreadsheets', 
                                                                'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())

gc = gspread.authorize(credentials)

spreadsheetId = '1-3K9usjWgwA8wcMxtPZcH4xyJ-psQZ3-meR5pau58ZI'
wks_name = 'Input_data'
d2g.upload(df, spreadsheetId, wks_name, credentials=credentials, row_names=True)

spreadsheetId = '1-3K9usjWgwA8wcMxtPZcH4xyJ-psQZ3-meR5pau58ZI'
wks_name = 'Output_data'
d2g.upload(outdataframe, spreadsheetId, wks_name, credentials=credentials, row_names=True)

