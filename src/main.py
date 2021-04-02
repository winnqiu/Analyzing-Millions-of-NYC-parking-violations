from os import environ
import sys
import argparse

import requests
import json
import threading

from requests.auth import HTTPBasicAuth
from sodapy import Socrata
from datetime import datetime
from time import time
from config import mappings
from helper import (
    ElasticException,
    insert_doc,
    create_index,
    get_rows_n_insert)
    

#create command line arguments page_size and num_pages

parser=argparse.ArgumentParser(description='process data from OPCV')
parser.add_argument('--page_size',type=int,help='how many rows to get per page',required=True)
parser.add_argument('--num_pages',type=int,help='how many pages to get in total')
args=parser.parse_args(sys.argv[1:])

#Get arguments needed from command line

DATASET_ID = environ.get('DATASET_ID') #"nc67-uf89"
APP_TOKEN = environ.get("APP_TOKEN") #"uQAFczuo1dJwPKRFeBy6yDQUf"
ES_HOST = environ.get("ES_HOST") 
ES_USERNAME = environ.get("ES_USERNAME")#"winn"
ES_PASSWORD = environ.get("ES_PASSWORD")#"Sta9760!"




if __name__ == '__main__':
        
    #connect to data source
    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )

    #print a single row of data to test the connection
    #rows = client.get(DATASET_ID, limit=1,order=":id")
    #print(rows)

    #connect to es with request module(print es info)
    #resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
    #print(resp.json())
    
    # Create index 'parking_violation'
    index='parking_violation'
    #try:
    #    resp=requests.delete(
    #        f'{ES_HOST}/{index}',
    #        auth=HTTPBasicAuth(ES_USERNAME,ES_PASSWORD)
    #    )
    #    resp.raise_for_status()
    #except:
    #    pass
    
    create_index(index,ES_HOST,ES_USERNAME,ES_PASSWORD,mappings)
    
    #return total number of rows in the dataset
    lines=client.get(DATASET_ID, select='count(*)')
    lines=int(lines[0]['count'])
    
    #initiate offset to 0, calculate number of API calls if num_pages not provided.
    offset=0
    limit=args.page_size
    if args.num_pages:
        num_API_calls=args.num_pages
    else:
        num_API_calls=int(lines/args.page_size)
    
    #determine number of treads
    if num_API_calls<5:
        num_threads=num_API_calls
    else:
        num_threads=5
    
    #determine number of loops
    num_of_loops=int(num_API_calls/num_threads)
    
    s0=time()
    for i in range(num_of_loops):
        threads=[]
        for j in range(num_threads):
            t=threading.Thread(
                target=get_rows_n_insert,
                args=(DATASET_ID,index,client,ES_HOST,ES_USERNAME,ES_PASSWORD,limit,offset,),
                )
            threads.append(t)
            #get_rows_n_insert(DATASET_ID,index,client,ES_HOST,ES_USERNAME,ES_PASSWORD,limit=args.page_size,offset=offset)
            offset+=limit
            t.start()
        
        for th in threads:
            th.join()
        
        print(f'{offset} rows inserted.')
        
        
    print(f"DONE {time()-s0}")