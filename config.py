import pandas as pd
import json
import uuid
import datetime
from rdflib import Namespace

with open('data/mapping.json') as f:
    term_mapping = json.load(f)
    
with open('data/vocabulary.json') as f:
    voca = json.load(f)
    
now = datetime.datetime.now()
for item in term_mapping:
    item['id'] = str(uuid.uuid1())
    item['issued'] = now.strftime('%Y-%m-%d %H:%M:%S')

TERM_MAPPING = pd.DataFrame(term_mapping)
TERM = pd.read_json('data/term.json')

ASSET_TERM_MAPPING = {item['column']: item['property_id'].replace(':','.')  for item in TERM_MAPPING.to_dict('records') if item['table']=='asset'}
COMPOSER_TERM_MAPPING = {item['column']: item['property_id'].replace(':','.')  for item in TERM_MAPPING.to_dict('records') if item['table']=='composer'}

store_namespaces = {item['name']: item['uri'] for item in voca}
decode_store_namespaces = {item['uri']: item['name'] for item in voca}


PREFIX = {key: Namespace(store_namespaces[key]) for key in store_namespaces.keys()}
PREFIX['kwangwoon'] = Namespace('https://www.kw.ac.kr/ko/')
EXTENDED_META = {
    'dataset': [
        'movement', 'ensemble', 'first', 'last', 'prev'
    ],
    'datasetseries': [
        'first', 'last', 'prev'
    ]
}

MIMETYPE = {
    'RDF/XML': 'application/rdf+xml',
    'Turtle': 'text/turtle',
    'N-Triples': 'application/n-triples',
    'JSON-LD': 'application/ld+json',
    'TriG': 'application/trig'
}