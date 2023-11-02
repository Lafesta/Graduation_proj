from pre_processing import pre_processing
import json
import pandas as pd
from encoder import Encoder
from decoder import Decoder
from rdflib import Graph
'''
dataset:
musicnet: https://www.kaggle.com/datasets/imsparsh/musicnet-dataset?resource=download&select=musicnet_metadata.csv
openopus: https://github.com/openopus-org/openopus_api/

'''

if __name__=='__main__':
    with open('data/composers.json', 'r') as f:
        composers = json.load(f)
        
    composers_df = pd.DataFrame(composers['composers'])
    composers_df.rename(columns={'name':'composer'}, inplace=True)
    
    meta_df = pd.read_csv('data/musicnet_metadata.csv')

    assets, composers = pre_processing(composers_df, meta_df)
    
    encoder = Encoder(assets, composers)
    
    encoder.generate_catalog(asset_list=assets, destination='sample.ttl', format="Turtle")
    
    decoder = Decoder()
    
    rdf = Graph()
    
    rdf.parse("sample.ttl", format='ttl')
    
    decoder.fit_transform(rdf=rdf)  
    
    decoder.print_value()