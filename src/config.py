mappings={
    "settings":{
        "number_of_shards":1,
        "number_of_replicas":1
        
    },
    "mappings":{
        "properties":{
            'plate': {"type":"text"},
            'state': {"type":"text"},
            'license_type': {"type":"text"},
            'summons_number': {"type":"long"}, 
            'issue_date': {"type":"date",
                           # "format":"MM/dd/YYYY"
            }, 
            'violation': {"type":"text"}, 
            'judgment_entry_date': {"type":"date",
                #'format':"MM/dd/YYYY"
            }, 
            'fine_amount': {"type":"float"}, 
            'penalty_amount': {"type":"float"}, 
            'interest_amount': {"type":"float"}, 
            'reduction_amount': {"type":"float"}, 
            'payment_amount': {"type":"float"}, 
            'amount_due': {"type":"float"}, 
            'precinct': {"type":"text"}, 
            'county': {"type":"text"},
            'issuing_agency': {"type":"text"},
            'violation_status': {"type":"text"}
                    }
                }
            }