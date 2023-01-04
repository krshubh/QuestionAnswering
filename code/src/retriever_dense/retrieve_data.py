import os
import sys
import json
import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch

def connect_es():
    """Function to connect to ElasticSearch on localhost on port 9200

    Returns:
        ElasticSearch: Return Elastic Search Client
    """
    username = "elastic"
    password = "jr=EzMHdB1evjVhbmNGB"
    client = Elasticsearch(hosts= [r"https://localhost:9200"], basic_auth=(username, password), verify_certs=False)
    if client.ping():
        print("Connected to ES")
    else :
        print("Could not connect!")
        sys.exit
    return client

class ContextSearch:

    def __init__(self, client, index_name):
        self.client = client
        self.index_name = index_name

    def get_keyword_response(self, query_text, size = 10):
        body = {"query": {"match":{"text":query_text}}}
        res = self.client.search(index = self.index_name, body=body, size=size)
        return res

    def search(self, query_text):
        responses = self.get_keyword_response(query_text=query_text)
        responses = responses["hits"]["hits"]
        for response in responses :
            print("score : ",response['_score'])
            print("context : ",response['_source']['text'])


if __name__ == "__main__" :
    client = connect_es()
    INDEX_NAME = "qa_dense"
    c_s = ContextSearch(client=client, index_name=INDEX_NAME)
    c_s.search("Norman")

