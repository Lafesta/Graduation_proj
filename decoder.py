import rdflib
from rdflib import Graph, BNode
from rdflib import URIRef, Literal
import uuid
import config as cfg
from pathlib import Path
import pandas as pd
import itertools
import json 

class Decoder():

    def __init__(self):
        self.reader = {}
        self.reader_type = {}
        self.store = [] # table 객체 저장
        self.TERM_MAPPING = None
        self.JSONB_TYPE_PROPERTIES = None
        self.bnode_store = []
        
        Asset.stored = []
        Composer.stored = []    

        self.store_predicate = set()
        
    def fit_transform(self, rdf):
        self.RDF = rdf

        self.uri_list = []
        self.bnode_list = []
        for triple in self.RDF:
            self.reader[str(triple[0])] = []

        for triple in self.RDF:
            subject, predicate, object = triple
            self.reader[str(subject)].append({'predicate': str(predicate), 'object': object})
            
            if str(predicate).split('#')[-1] == 'type':
                if self.discriminate_bnode_url(str(subject)):
                    self.reader_type[self.discriminate_bnode_url(str(subject))] = str(object).split('#')[-1]
                
        # bnode와 uri check
        for uri in self.reader.keys():
            if self.discriminate_bnode_url(uri):
                self.uri_list.append(self.discriminate_bnode_url(uri))
            else:
                self.bnode_list.append(uri)
                
        # for uri in self.bnode_list:
        #     print("bnode data: ",self.reader[uri])
    
        for uri in self.uri_list:
            splitted_uri = uri.split('/')
            if splitted_uri[-2] == 'asset':
                if uri in Asset.stored:
                    print("Asset already stored")
                    return
                else:
                    asset = Asset(uri)
                    self.get_metadata_from_rdf(table_name='asset', instance=asset, rdf_data=self.reader[uri], instance_type=self.reader_type[uri])
                    self.store.append(asset)
                    
            if splitted_uri[-2] == 'composer':
                if uri in Composer.stored:
                    print("Composer already stored")
                    return
                else:
                    dist = Composer(uri)
                    dist.set('id', self.discriminator(URIRef(uri)))
                    self.get_metadata_from_rdf(table_name='composer', instance=dist, rdf_data=self.reader[uri], instance_type=self.reader_type[uri])
                    self.store.append(dist)
                    
    def get_metadata_from_rdf(self, table_name:str, instance, rdf_data, instance_type):
        if table_name == 'asset':
            self.TERM_MAPPING = cfg.TERM_MAPPING.loc[cfg.TERM_MAPPING['table']=='asset', :]
            instance.set('asset_type', instance_type.lower())
            # print(self.TERM_MAPPING)
            
        if table_name == 'composer':
            self.TERM_MAPPING = cfg.TERM_MAPPING.loc[cfg.TERM_MAPPING['table']=='composer', :]

        for item in rdf_data:
            # if table_name == 'asset':
            #     print(item)
            self.store_predicate.add(item['predicate'])
            pathlike_uri = Path(item['predicate'])
            if "#" in pathlike_uri.name:
                ns = "#".join(item['predicate'].split("#")[0:-1]) + "#"
                
            else:
                ns = "/".join(item['predicate'].split("/")[0:-1]) + "/"
                
            property = item['predicate'].replace(ns, '')
            ns = cfg.decode_store_namespaces[ns]    
            # print(ns, property)
            
            predicate = ns + ':' + property
            properties = self.TERM_MAPPING.loc[:, 'property_id']
                                    
            for prop in properties:
                # print("key: ", key)
                temp = []
                
                if prop == predicate:
                    # print("dd")
                    mapping = self.TERM_MAPPING.loc[self.TERM_MAPPING['property_id']==prop, 'column':'class_id']

                    for idx, row in mapping.iterrows():
                        # print(row)
                        temp_dict = {}
                        temp_dict['column'] = row[0]
                        temp_dict['class_id'] = row[1]
                        temp.append(temp_dict)
                    
                    for i in temp:
                        if item['object'] == 'NULL':
                            print(f"text: {item['object']}, column: {i['column']}")
                        # print(f"text: {item['object']}, column: {i['column']}")
                        instance.set(i['column'], self.discriminator(value=item['object']))
    
    def discriminate_bnode_url(self, uri):
        splitted_uri = uri.split('/')
        if len(splitted_uri) >= 2:
            return uri
        else:
            return None     
    
    def discriminator(self, value):
        try:
            if value == None:
                return None
            
            stripped_value = str(value).strip()
            
            if (isinstance(value, URIRef) and (stripped_value.startswith("http://")
                                            or stripped_value.startswith("https://"))):
                return stripped_value.split('/')[-1]
            else:
                return str(value)
        except Exception:
            return str(value)                   

    def print_value(self):
        # for instance in self.store:
        #     print(instance.columns)
        #     print()
        with open('test_decoder.json', 'w') as f:
            for instance in self.store:
                json.dump(instance.columns, f, indent=4, ensure_ascii=False)
                # print()
        for i in self.store_predicate:
            print(i)
class Asset():
    stored = []
    def __init__(self, uri):
        Asset.stored.append(uri)
        
        self.columns = {
            'id':str(uuid.uuid1()),
            'composition': None,
            'composer': None,
            'movement': None,
            'epoch': None,
            'ensemble': None,
            'catalog': None, # in series
            'first': None,
            'last': None,
            'prev': None,
            'asset_type': None 
        }
        
    def set(self, key, value):
        if isinstance(value, list) and isinstance(self.columns[key], list) == False:
            self.columns[key] = value  
        if isinstance(value, dict):
            self.columns[key].append(value)
        elif isinstance(value, str) and isinstance(self.columns[key], list) and value != 'NULL':
            self.columns[key].append(value)
        else:
            self.columns[key] = value

class Composer():
    stored = []
    
    def __init__(self, uri):
        Composer.stored.append(uri)
        
        self.columns = {
            'id': str(uuid.uuid1()),
            'uri': None,
            'composer': None,
            'epoch': None,
            'birth': None,
            'death': None,
        }
        
    def set(self, key, value):
        if isinstance(value, list) and isinstance(self.columns[key], list) == False:
            self.columns[key] = value  
        if isinstance(value, dict):
            self.columns[key].append(value)
        elif isinstance(value, str) and isinstance(self.columns[key], list) and value != 'NULL':
            self.columns[key].append(value)
        else:
            self.columns[key] = value