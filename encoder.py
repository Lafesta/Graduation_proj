from rdflib import Graph, BNode
from rdflib import URIRef, Literal
import json
from urllib.parse import quote
import argparse
import config as cfg
import pandas as pd
import itertools

class Encoder():
    
    def __init__(self, assets, composers):
        self.store = Graph()
        self.asset_table = pd.DataFrame(assets)
        self.composer_table = pd.DataFrame(composers)
        self.comp_uri = None
        
    def generate_catalog(self, asset_list:list=None, destination:str=None, format:str='RDF/XML'):
        catalog_uri = URIRef(cfg.PREFIX['kwangwoon'] + 'catalog')
        
        self.store.add((catalog_uri, cfg.PREFIX['rdf'].type, cfg.PREFIX['dcat'].Catalog))
        
        if type(asset_list) != list:
            lst = []
            lst.append(asset_list)
            asset_list = lst
        for asset in asset_list:
            self.generate_asset(store=self.store, asset=asset, catalogID=catalog_uri, direct=True)
        
        self.store.serialize(destination=destination, format=cfg.MIMETYPE[format])
        
    def generate_asset(self, store:Graph, catalogID:str, instance:bool=True, direct:bool=False, asset=None):
        
        BASIC_PROPERTIES = [key for key in asset.keys() if key not in list(itertools.chain(*cfg.EXTENDED_META.values()))]
        
        asset_uri = URIRef('{}asset/{}'.format(cfg.PREFIX['kwangwoon'], asset['id']))
        if self.is_exist(store=store, uri=asset_uri) == True:
            if direct == True:
                if asset['asset_type'] == 'dataset':
                    store.add((catalogID, cfg.PREFIX['dcat'].dataset, asset_uri))
                if asset['asset_type'] == 'datasetseries':
                    store.add((catalogID, cfg.PREFIX['dcat'].dataset, asset_uri))
                
        if self.is_exist(store=store, uri=asset_uri) == False:
            if asset['asset_type'] == 'dataset':
                if instance == True or direct == True:
                    store.add((catalogID, cfg.PREFIX['dcat'].dataset, asset_uri))
                store.add((asset_uri, cfg.PREFIX['rdf'].type, cfg.PREFIX['dcat'].Dataset))
            if asset['asset_type'] == 'datasetseries':
                if instance == True or direct == True:
                    store.add((catalogID, cfg.PREFIX['dcat'].dataset, asset_uri))
                store.add((asset_uri, cfg.PREFIX['rdf'].type, cfg.PREFIX['dcat'].DatasetSeries))
                
                
            for key in asset.keys():
                if key not in cfg.ASSET_TERM_MAPPING.keys():
                    cfg.ASSET_TERM_MAPPING[key] = None
                    print(f'Table: asset, key: \"{key}\" doesn\'t exist in term_mapping table.')
                if cfg.ASSET_TERM_MAPPING[key] != None:
                    ns, prop = cfg.ASSET_TERM_MAPPING[key].split('.')
                    predicate = URIRef(cfg.PREFIX[ns] + prop)
                    
                    if key in BASIC_PROPERTIES:
                        if key == 'composer':
                            self.get_composer(store=store, composer=asset[key], uri=asset_uri, predicate=predicate)
                        else:
                            if asset[key] == None:
                                store.add((asset_uri, predicate, self.discriminator(None)))
                            else:
                                store.add((asset_uri, predicate, self.discriminator(asset[key])))
                    elif key in cfg.EXTENDED_META[asset['asset_type']]:
                        if asset['asset_type'] == 'datasetseries':
                            if asset[key] != None:
                                if self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records'):
                                    self.generate_asset(store=store, asset=self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records')[0], catalogID=asset_uri, instance=False)
                        else:
                            if asset['catalog'] != None and key in ['first', 'last', 'prev']:
                                if asset[key] != None:
                                    if self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records'):
                                        self.generate_asset(store=store, asset=self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records')[0], catalogID=asset_uri, instance=False)
                        
                        if asset[key] == None:
                            store.add((asset_uri, predicate, self.discriminator(None)))
                        else:
                            if key in ['first', 'last', 'prev']:
                                if self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records'):
                                    series_asset = self.asset_table.loc[self.asset_table['id']==asset[key]].to_dict('records')[0]
                                    series_uri = URIRef('{}asset/{}/{}'.format(cfg.PREFIX['kwangwoon'], series_asset['asset_type'], series_asset['id']))
                                    store.add((asset_uri, predicate, self.discriminator(series_uri)))
                            else:
                                store.add((asset_uri, predicate, self.discriminator(asset[key])))
                            
    
    def get_composer(self, store, composer, uri, predicate):
        if self.composer_table.loc[self.composer_table['composer']==composer, 'uri'].values:
            self.comp_uri = URIRef(self.composer_table.loc[self.composer_table['composer']==composer, 'uri'].values[0])
        
        if self.is_exist_comp(store, self.comp_uri) == False:
            store.add((self.comp_uri, cfg.PREFIX['rdf'].type, cfg.PREFIX['foaf'].Person))
            
            meta = self.composer_table.loc[self.composer_table['composer']==composer].to_dict('records')[0]
            
            for key in meta.keys():
                if key not in cfg.COMPOSER_TERM_MAPPING.keys():
                    cfg.COMPOSER_TERM_MAPPING[key] = None
                    print(f'Table: composer, key: \"{key}\" doesn\'t exist in term_mapping table.')
                if cfg.COMPOSER_TERM_MAPPING[key] != None:
                    ns, prop = cfg.COMPOSER_TERM_MAPPING[key].split('.')
                    pred = URIRef(cfg.PREFIX[ns] + prop)

                    if meta[key] == None:
                        store.add((self.comp_uri, pred, self.discriminator(None)))
                    else:
                        store.add((self.comp_uri, pred, self.discriminator(meta[key])))
        
        store.add((uri, predicate, self.comp_uri))
        
    def discriminator(self, value, language:str=None, datatype:URIRef=None):
        '''
        object가 Literal value 인지 URI value인지 판별
        '''
        try:
            if value == None:
                return Literal('NULL')
                
            stripped_value = value.strip()

            if (isinstance(value, str) and (stripped_value.startswith("http://")
                                                or stripped_value.startswith("https://"))):
                uri_obj = self.cleaned_URIRef(value)
                # although all invalid chars checked by rdflib should have been quoted, try to serialize
                # the object. If it breaks, use Literal instead.
                uri_obj.n3()
                # URI is fine, return the object
                return uri_obj
            else:
                return Literal(value, lang=language, datatype=datatype)
        except Exception:
            # In case something goes wrong: use Literal
            return Literal(value, lang=language, datatype=datatype)

    def cleaned_URIRef(self, value) -> URIRef:
        if isinstance(value, str):
            value = self.careful_quote(value.strip())
        return URIRef(value)

    def careful_quote(self, value) -> str:
        # only encode this limited subset of characters to avoid more complex URL parsing
        # (e.g. valid ? in query string vs. ? as value).
        # can be applied multiple times, as encoded %xy is left untouched. Therefore, no
        # unquote is necessary beforehand.
        quotechars = ' !"$\'()*,;<>[]{|}\\^`'
        
        for c in quotechars:
            value = value.replace(c, quote(c))
        
        return value

    def is_exist(self, store:Graph, uri:URIRef) -> bool:
        query = """
        SELECT ?dataset
        WHERE {
            ?dataset a dcat:Dataset .
        }
        """
        result = store.query(query)
        # print(type(result))
        is_exist = False
        
        # print(len(result))
        for row in result:
            # print(key)
            # print("asset[key]: ", str(series_data['uri']))
            # print("row: ", str(row.dataset))
            if str(uri) == str(row.dataset):
                # print("equal")
                is_exist = True
                break
        return is_exist
    
    def is_exist_comp(self, store:Graph, uri:URIRef) -> bool:
        query = """
        SELECT ?creator
        WHERE {
            ?creator a foaf:Person .
        }
        """
        result = store.query(query)
        # print(type(result))
        is_exist = False
        
        # print(len(result))
        for row in result:
            # print(key)
            # print("asset[key]: ", str(series_data['uri']))
            # print("row: ", str(row.dataset))
            if str(uri) == str(row.creator):
                # print("equal")
                is_exist = True
                break
        return is_exist