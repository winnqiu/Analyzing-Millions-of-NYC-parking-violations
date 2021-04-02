import requests

from requests.auth import HTTPBasicAuth
from datetime import datetime
from time import time

class ElasticException(Exception):
    pass

def create_index(index,host,username=None,password=None,mappings=None):
    if not username or not password:
        raise ElasticException("Your username and password is required!")
    
    if mappings is None or len(mappings.keys())==0:
        raise ElasticException("Mappings required!")
    
    try:
        resp=requests.put(f"{host}/{index}",
        auth=HTTPBasicAuth(username,password),
        json=mappings
            )
        resp.raise_for_status()
    except Exception as e:
        raise ElasticException(f"Index already exists! error message:{e}")
        
def insert_doc(index,host,data=None, username=None,password=None):
    if not username or not password:
        raise ElasticException("Your username and password is required!")
    
    if data is None or len(data.keys())==0:
        raise ElasticException("This data is empty")
        
    try:
        resp=requests.post(
            f"{host}/{index}/_doc",
            auth=HTTPBasicAuth(username,password),
            json=data
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise ElasticException(f"Failed to create document:{e}")
        
def get_rows_n_insert(DATASET_ID,index,client,host,username,password,limit=10,offset=0,order=':id'):
    s1=time()
    rows = client.get(DATASET_ID, limit=limit,offset=offset,order=":id")
    for row in rows:
        keys=['plate','state','license_type','summons_number','issue_date','violation','judgment_entry_date','fine_amount','penalty_amount','interest_amount','reduction_amount','payment_amount','amount_due','precinct','county','issuing_agency','violation_status']
        row=dict((k, row[k]) for k in keys if k in row)
        try:
            row['issue_date']=datetime.strptime(row['issue_date'],'%m/%d/%Y')
            row['issue_date']=datetime.strftime(row['issue_date'],'%Y-%m-%d')
            if 'summons_number' in row:
                row['summons_number']=int(row['summons_number'])
            if 'fine_amount' in row:
                row['fine_amount']=float(row['fine_amount'])
            if 'penalty_amount' in row:
                row['penalty_amount']=float(row['penalty_amount'])
            if 'interest_amount' in row:
                row['interest_amount']=float(row['interest_amount'])
            if 'reduction_amount' in row:
                row['reduction_amount']=float(row['reduction_amount'])
            if 'payment_amount' in row:
                row['payment_amount']=float(row['payment_amount'])
            if 'amount_due' in row:
                row['amount_due']=float(row['amount_due'])
            if 'judgment_entry_date' in row:
                row['judgment_entry_date']=datetime.strptime(row['judgment_entry_date'],'%m/%d/%Y')
                row['judgment_entry_date']=datetime.strftime(row['judgment_entry_date'],'%Y-%m-%d')
        except Exception as e:
            print(f"Skipping! Failed to transform row: {row}. Reason: {e}")
            continue
        try:
            line=insert_doc(index,host,row,username,password)
            print(line)
        except ElasticException as e:
            print(e)
    








