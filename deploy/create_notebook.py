"""Creates notebooks
"""


import json
import os
import sys


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

NOTEBOOK_SKELETON_PREFIX = '{}/output/notebook'.format(PROJECT_ROOT_PATH)
PARAGRAPH_PREFIX = '{}/notebooks'.format(PROJECT_ROOT_PATH)
OUTPUT_PREFIX = '{}/output/notebook'.format(PROJECT_ROOT_PATH)

paragraphs = {
    'global-loads': {
        'title': 'Carregar variáveis globais',
        'index': 1,
        'filename': 'global-loads.py'
    },
    'table-loads': {
        'title': 'Carregar tabelas',
        'index': 2,
        'filename': 'table-loads.py'
    },
    'enum-loads': {
        'title': 'Carregar enumerações',
        'index': 3,
        'filename': 'enum-loads.py'
    },
    'query': {
        'title': 'Consultar reclamações',
        'index': 4,
        'filename': 'query.py'
    }
}


notebook_name = sys.argv[1]
paragraph_name = sys.argv[2]

paragraph = paragraphs[paragraph_name]


notebook_conf = json.load(open('{}/{}/note.json'.format(NOTEBOOK_SKELETON_PREFIX, notebook_name)))


paragraph_path = '{}/{}/{}'.format(PARAGRAPH_PREFIX, notebook_name, paragraph['filename'])
file = open(paragraph_path)
text = file.read()


notebook_conf["paragraphs"][paragraph['index']]["title"] = paragraph['title']
notebook_conf["paragraphs"][paragraph['index']]["text"] = text


output_path = '{}/{}'.format(OUTPUT_PREFIX, notebook_name)
if not os.path.exists(output_path):
    os.makedirs(output_path)
json.dump(notebook_conf, open('%s/note.json' % output_path, 'w'), indent=4)
