import json
import pandas as pd
import uuid
import config as cfg
pd.set_option('display.max_colwidth', 20)

def pre_processing(composers_df, meta_df):
    assets = []    
        
    for _, music in meta_df.iterrows():
        asset = { # composition
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

        asset['composition'] = music['composition']

        asset['composer'] = music['composer']
        asset['movement'] = music['movement']
        if composers_df.loc[composers_df['composer']==asset['composer'], 'epoch'].values:
            asset['epoch'] = composers_df.loc[composers_df['composer']==asset['composer'], 'epoch'].values[0]
  
        asset['ensemble'] = music['ensemble']
        asset['catalog'] = music['catalog_name']
        asset['first'] = meta_df.loc[meta_df['composition']==asset['composition'], 'seconds'].iloc[0]
        asset['last'] = meta_df.loc[meta_df['composition']==asset['composition'], 'seconds'].iloc[-1]
        asset['asset_type'] = 'dataset'
        # asset['prev'] = meta.loc[meta['composition']==asset['asset_id'], 'seconds'].index

        assets.append(asset)
    for c in meta_df['composition'].unique():    
        if len(meta_df.loc[meta_df['composition']==c]) > 1:
            temp_asset = meta_df.loc[meta_df['composition']==c].iloc[0]
            temp = {}
            temp['id'] = str(uuid.uuid1())
            temp['composition'] = c
            temp['composer'] = temp_asset['composer']
            if composers_df.loc[composers_df['composer']==temp['composer'], 'epoch'].values:
                temp['epoch'] = composers_df.loc[composers_df['composer']==temp['composer'], 'epoch'].values[0]
            temp['catalog'] = temp_asset['catalog_name']
            
            for col in cfg.EXTENDED_META['dataset']:
                temp[col] = None
            temp['asset_type'] = 'datasetseries'
            assets.append(temp)
    assets = pd.DataFrame(assets)

    for c in assets['composition'].unique():
        assets.loc[(assets['composition']==c) & (assets['asset_type']=='dataset'), 'first'] = assets.loc[(assets['composition']==c) & (assets['asset_type']=='dataset'), 'id'].iloc[0]
        assets.loc[(assets['composition']==c) & (assets['asset_type']=='dataset'), 'last'] = assets.loc[(assets['composition']==c) & (assets['asset_type']=='dataset'), 'id'].iloc[-1]
    
    prev=None
    composition=None
    for _, item in assets.iterrows():
        if item['composition'] == composition:
            assets.loc[(assets['id']==item['id']) & (assets['asset_type']=='dataset'), 'prev'] = prev
        prev, composition=item['id'], item['composition'] 
        
        
    # print(assets.loc[assets['catalog']=='OP114'])
    
    composers = []
    
    for _, comp in composers_df.iterrows():
        composer = {
            'id': str(uuid.uuid1()),
            'uri': None,
            'composer': None,
            'epoch': None,
            'birth': None,
            'death': None,
        }
        
        composer['uri'] = cfg.PREFIX['kwangwoon'] + 'composer/' + comp['composer']
        composer['composer'] = comp['composer']
        composer['epoch'] = comp['epoch']
        composer['birth'] = comp['birth']
        composer['death'] = comp['death']
        
        composers.append(composer)
    composers = pd.DataFrame(composers)
    assets.fillna(value='NULL', inplace=True)
    composers.fillna(value='NULL', inplace=True)
    # for c in composers['composer'].unique():
    #     print(c)
    #     print(assets.loc[assets['composer']==c, :])
    
    return assets.to_dict('records'), composers.to_dict('records')
