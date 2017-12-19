"""Prototype of a visualization system
"""


import json
import requests as r
import sys


BASE_URL = 'http://10.0.1.50:8080/api'

NOTEBOOK_NAME = 'blackdog'
PARAGRAPH_TEXT = r"""%jdbc
SELECT
    to_char(year, 'YYYY') AS year,
    new_complains_amount,
    new_users_amount
FROM
    raichu_novelties.yearly_counts
WHERE
    year >= '2002-01-01 00:00:00'
ORDER BY
    year"""


def get_notebook_id():
    url = '{}/notebook'.format(BASE_URL)
    resp = r.get(url)

    if resp.status_code != 200:
        sys.exit()

    notebooks = resp.json()['body']
    notebooks = [notebook['id'] for notebook in notebooks if notebook['name'] == NOTEBOOK_NAME]
    if not notebooks:
        return None
    return notebooks[0]


def create_notebook():
    url = '{}/notebook'.format(BASE_URL)
    resp = r.post(url, json.dumps({
        'name': NOTEBOOK_NAME
    }))

    if resp.status_code != 201:
        sys.exit()

    return get_notebook_id()


def get_paragraph_id(notebook_id):
    url = '{}/notebook/job/{}'.format(BASE_URL, notebook_id)
    resp = r.get(url)

    if resp.status_code != 200:
        sys.exit()

    body = resp.json()['body']
    if not body:
        return None

    return body[0]['id']


def delete_paragraph(notebook_id, paragraph_id):
    url = '{}/notebook/{}/paragraph/{}'.format(BASE_URL, notebook_id, paragraph_id)
    resp = r.delete(url)


def create_paragraph(notebook_id):
    url = '{}/notebook/{}/paragraph'.format(BASE_URL, notebook_id)
    resp = r.post(url, json.dumps({
        'title': '',
        'text': PARAGRAPH_TEXT
    }))

    if resp.status_code != 201:
        sys.exit()

    return get_paragraph_id(notebook_id)


def run_paragraph(notebook_id, paragraph_id):
    url = '{}/notebook/run/{}/{}'.format(BASE_URL, notebook_id, paragraph_id)
    resp = r.post(url, json.dumps({
    }))

    if resp.status_code != 200:
        sys.exit()

    print(resp.json())


notebook_id = get_notebook_id()
if not notebook_id:
    notebook_id = create_notebook()
print("ID for notebook '{}': {}".format(NOTEBOOK_NAME, notebook_id))

paragraph_id = get_paragraph_id(notebook_id)
delete_paragraph(notebook_id, paragraph_id)
paragraph_id = create_paragraph(notebook_id)
print("ID for paragraph: {}".format(paragraph_id))

run_paragraph(notebook_id, paragraph_id)
