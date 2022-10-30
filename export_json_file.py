import datetime
import elasticsearch
import json

es = elasticsearch.Elasticsearch('http://localhost:9200')

def get_page(inp):
    print(inp)

    res = es.search(index="index_name", body={"from":int(inp),"size":1000,
    "query": {
	    "bool": {
	      "must" : [ 
		{ "terms": { "data_type.keyword": ["input_type"] } },
        { "range": {"date": {"gte": "now-30d/d"}}} 
	      ]
	    }
	  } 

    })
    return  res["hits"]["hits"]


if __name__ == '__main__':
    print ("Exporting data started at " + str(datetime.datetime.now()))
    start = 0
    page = get_page(start)
    data = []
    while len(page) > 0:
    ##### CALCULATION LOGIC HERE####
        print ("Exporting batch number: " + str((start / 1000)+1) +" Starting from doc number: " + str(start))
        for doc in page:
            data.append(doc)
        start= start + 1000
        page = get_page(start)
    print(len(data))
    with open('/Download/export_json_data.json', 'w') as outfile:
     json.dump(data, outfile)

    print ("Exporting data finished at "+str(datetime.datetime.now()))

