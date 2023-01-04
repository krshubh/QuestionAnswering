import sys
import json
import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datasets import Dataset, DatasetDict

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

class IndexData:
    """
    IndexData Class
    """
    def __init__(self, client, index_name):
        """
        Function to index QuestionAnswer Data

        Args:
            client (object): Elasticsearch client
            index_name (string): Name of the index in which data is indexed.
        """
        self.client = client
        self.index_name = index_name

    def index_data(self, data_file):
        """
        Function to do index data in elasticsearch
        and preprocessing per data require

        Args:
            data_file (string): data file name
        """
        docs = []
        cnt = 0
        with open(data_file, 'rb') as full_data:
            docs = [json.loads(i)['context'] for i in full_data]
            context_dict = {"context" : docs}
            train_dataset = Dataset.from_dict(context_dict)
            train_dataset.add_elasticsearch_index(column = 'context', es_client = self.client, es_index_name = self.index_name)

if __name__ == "__main__":
    DATA_FILE = r"data/train_context.jsonl"
    INDEX_FILE = r"code/src/retriever_dense/index.json"
    client = connect_es()
    q_s = IndexData(client=client, index_name='qa_dense')
    q_s.index_data(data_file = DATA_FILE)



