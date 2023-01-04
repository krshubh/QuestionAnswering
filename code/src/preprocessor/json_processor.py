import json
from tqdm import tqdm


def dump_jsonl(data, output_path, append=False):
    """
    Helper function to write the data
    Args:
        data(list): list of data
        output_path(path): path of the output dir
    """
    mode = 'a+' if append else 'w'
    with open(output_path, mode, encoding='utf-8') as f:
        for line in data:
            json_record = json.dumps(line, ensure_ascii=False)
            f.write(json_record + '\n')
    f.close()
    print(f'Wrote {len(data)} records to {output_path}')

def process_document(data_file, output_file):
    """
    Helper function to write
    processed doc in jsonl format

    Args:
        data_file (json): squad json file
    """
    docs = []
    count = 1
    with open(data_file, 'rb') as file :
        data = json.load(file)
    data_list = data['data']
    for data in tqdm(data_list):
        paragraphs = data['paragraphs']
        for paragraph in paragraphs:
            context = paragraph['context']
            m_doc = {'context': context}
            m_doc['_id'] = count
            m_doc['context'] = context
            docs.append(m_doc)
            count += 1
    dump_jsonl(docs, output_path=output_file)


if __name__ == "__main__" :
    TRAIN_JSON_FILE = r"data/train-v2.0.json"
    process_document(data_file = TRAIN_JSON_FILE, output_file = r'data/train_context.jsonl')
    TEST_JSON_FILE = r"data/dev-v2.0.json"
    process_document(data_file = TEST_JSON_FILE, output_file = r'data/dev_context.jsonl')
