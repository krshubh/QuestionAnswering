import sys
import json
import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

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

    def create_index(self, index_file):
        """
        Function to create new index with new name
keyword_analyzerkeywkeyword_analyzerord_analyzer
        Args:
            index_file (String): Index file name
        """
        self.index_file = index_file
        print("Creating index.")
        with open(self.index_file, 'rb') as index:
            body = json.loads(index.read().strip())
            ret = self.client.indices.create(
                index=self.index_name,
                settings=body['settings'],
                mappings=body['mappings'],
                ignore=400
            )
            print("Return Response of Create Index ", ret)

        

    def index_batch(self, docs):
        """
        Helper fuction to index the Documents

        Args:
            docs (List): document list of json

        Returns:
            json: Return response
        """
        requests = []                
        for _, doc in enumerate(docs):
            request = doc
            request['_index'] = self.index_name
            request['_op_type'] = "index"
            requests.append(request)
        response = bulk(self.client, requests, index=self.index_name)
        return response

    def index_data(self, data_file, batch_size):
        """
        Function to do index data in elasticsearch
        and preprocessing per data require

        Args:
            data_file (string): data file name
            batch_size (int): batch size of processing
        """
        docs = []
        cnt = 0
        with open(data_file, 'rb') as full_data:
            for i in full_data:
                data = json.loads(i)
                docs.append(data)
                cnt += 1
                if cnt % batch_size == 0 :
                    _ = self.index_batch(docs)
                    docs = []
                    print(f'Indexed {cnt} documents')
            _ = self.index_batch(docs)
            print(f"Indexed {cnt} documents")

if __name__ == "__main__":
    DATA_FILE = r"data/train_context.jsonl"
    INDEX_FILE = r"code/src/retriever_es/index.json"
    BATCH_SIZE = 1000
    client = connect_es()
    q_s = IndexData(client=client, index_name='qa')
    q_s.create_index(INDEX_FILE)
    q_s.index_data(data_file = DATA_FILE, batch_size = BATCH_SIZE)



