"""Creates notebooks
"""


import json
import os
import sys


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

NOTEBOOK_SKELETON_PREFIX = '{}/output/notebook'.format(PROJECT_ROOT_PATH)
PARAGRAPH_PREFIX = '{}/notebooks'.format(PROJECT_ROOT_PATH)
OUTPUT_PREFIX = '{}/output/notebook'.format(PROJECT_ROOT_PATH)

indices = {
    'table-loads.py': 1,
    'query.py': 2
}


notebook_name = sys.argv[1]
paragraph_filename = sys.argv[2]


notebook_conf = json.load(open('{}/{}/note.json'.format(NOTEBOOK_SKELETON_PREFIX, notebook_name)))


paragraph_path = '{}/{}/{}'.format(PARAGRAPH_PREFIX, notebook_name, paragraph_filename)
file = open(paragraph_path)
text = file.read()


notebook_conf["paragraphs"][indices[paragraph_filename]]["text"] = text


output_path = '{}/{}'.format(OUTPUT_PREFIX, notebook_name)
if not os.path.exists(output_path):
    os.makedirs(output_path)
json.dump(notebook_conf, open('%s/note.json' % output_path, 'w'), indent=4)
