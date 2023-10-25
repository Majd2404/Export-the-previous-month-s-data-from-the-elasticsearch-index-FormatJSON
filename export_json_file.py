import datetime
import elasticsearch
import json

# Establish a connection to Elasticsearch
es = elasticsearch.Elasticsearch('http://localhost:9200')

# Function to get a page of data from Elasticsearch
def get_page(start):
    print("Fetching data starting from document number:", start)

    # Elasticsearch query to retrieve data based on specified criteria
    res = es.search(index="index_name", body={
        "from": int(start),
        "size": 1000,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"data_type.keyword": ["input_type"]}},
                    {"range": {"date": {"gte": "now-30d/d"}}}
                ]
            }
        }
    })
    return res["hits"]["hits"]

if __name__ == '__main__':
    print("Exporting data started at", str(datetime.datetime.now()))

    start = 0
    page = get_page(start)
    data = []

    while len(page) > 0:
        # CALCULATION LOGIC HERE (not provided in the original script)

        print("Exporting batch number:", (start / 1000) + 1, "Starting from doc number:", start)
        for doc in page:
            data.append(doc)

        start = start + 1000
        page = get_page(start)

    print("Total documents exported:", len(data))

    # Write the exported data to a JSON file
    with open('/Download/export_json_data.json', 'w') as outfile:
        json.dump(data, outfile)

    print("Exporting data finished at", str(datetime.datetime.now()))
