import pandas as pd
pd.set_option('display.max_columns', 20)
data = {}
data['track_id'] = []
data['track'] = []

with open('track_id_name_list.txt', 'r') as f:
    for i in f:
        splitted = i.split(' ')
        data['track_id'].append(splitted[0])
        data['track'].append(' '.join(splitted[1:])[:-1])

df = pd.DataFrame(data) 

df['track'] = df['track'].str.replace(pat=r'[^\w]', repl=r' ', regex=True).astype(dtype=str)

import nltk
nltk.download('popular')

df['tokenized_track'] = df['track'].apply(nltk.word_tokenize)

temp_dict = {}
temp_dict['org_id'] = []
temp_dict['remap_id'] = []
with open('composition_list.txt', 'r') as f:
    for i in f:
        splitted = i.split(' ')
        temp_dict['remap_id'].append(splitted[-1][:-1])
        temp = []
        splitted = ' '.join(splitted[:-1]).replace('_', ' ').split(' ')
        for word in splitted:
            if len(word) == 1:
                word = word + '_'
            temp.append(word)
        temp_dict['org_id'].append(' '.join(temp))

composition = pd.DataFrame(temp_dict)

composition = composition.iloc[1:,:]

composition['org_id'] = composition['org_id'].str.replace(pat=r'[^\w]', repl=r' ', regex=True).astype(dtype=str)
composition['tokenized_org_id'] = composition['org_id'].apply(nltk.word_tokenize)
word2index={}
for idx, voca in composition.iterrows():
    for token in voca['tokenized_org_id']:
        if token not in word2index.keys():
            if len(token)==1:
                token=token+'_'
            word2index[token.lower()] = len(word2index)
print(word2index)

import numpy as np
label = word2index.keys()
label = np.array(list(label))

from sklearn.feature_extraction.text import CountVectorizer

encoder = CountVectorizer(vocabulary=label, lowercase=True)
encoder.fit(label)

vectorized_composition = {}
for idx, row in composition.iterrows():
    temp = []
    temp.append(row['org_id'].lower())
    encoded_composition = encoder.transform(temp).toarray()
    vectorized_composition[row['remap_id']] = encoded_composition[0]

comp_df = pd.DataFrame(vectorized_composition)
comp_df = comp_df.T
comp_df.rename(columns={v:k for k, v in encoder.vocabulary_.items()}, inplace=True)
convert_index = {val['remap_id']:val['org_id'] for idx, val in composition.iterrows()}
comp_df.rename(index=convert_index, inplace=True)

convert_index = {val['remap_id']:val['org_id'] for idx, val in composition.iterrows()}
encoded_token = encoder.transform(['piano']).toarray()

temp_dict = {}
temp_dict['track'] = []

for idx, i in df.iterrows():
    splitted = i['track'].split(' ')
    temp = []
    splitted = ' '.join(splitted[:-1]).replace('_', ' ').split(' ')
    for word in splitted:
        if len(word) == 1:
            word = word + '_'
        temp.append(word)
    temp_dict['track'].append(' '.join(temp))
df['track'] = temp_dict['track']

vectorized_track = {}
for idx, row in df.iterrows():
    temp = []
    temp.append(row['track'].lower())
    encoded_track = encoder.transform(temp).toarray()[0]
    vectorized_track[row['track_id']] = encoded_track
track_df = pd.DataFrame(vectorized_track)
track_df = track_df.T
track_df.rename(columns={v:k for k, v in encoder.vocabulary_.items()}, inplace=True)
track_df = track_df.reset_index(0)
track_df.rename(columns={'index':'track_id'}, inplace=True)
track_df['track'] = df.loc[df['track_id']==track_df['track_id'], 'track']
use_track=track_df.drop(index=track_df.loc[track_df.iloc[:,1:-1].sum(axis=1)<=2].index)

comp_vec = comp_df.loc[:,:]
track_vec = use_track.iloc[:,:-1].set_index('track_id')

from sklearn.metrics import jaccard_score

# y_true = comp_vec.loc[:,:].T
# y_pred = track_vec.loc[:,:].T
# cnt=0
# jaccard = {}
# print(len(y_pred.columns) * len(y_true.columns))
# for i in range(len(y_pred.columns)):
#     jaccard[y_pred.columns[i]] = []
#     for j in range(len(y_true.columns)):
#         similarity = jaccard_score(y_true.iloc[:, j], y_pred.iloc[:, i], average=None)
#         jaccard[y_pred.columns[i]].append(similarity[1])
#         cnt+=1
#         if cnt%1000 == 0:
#             print(cnt)
#         # print(f"Jaccard similarity between {y_true.columns[j]} and {y_pred.columns[i]}: {similarity[1]}")
# jaccard_df = pd.DataFrame(jaccard).T



# 5시간 걸림
y_true = comp_vec.loc[:,:].T
y_pred = track_vec.loc[:,:].T
sample_weight = [len(w) for w in label]
print(sample_weight)
jaccard = {}
cnt=0
for i in range(len(y_pred.columns)):
    jaccard[y_pred.columns[i]] = []
    for j in range(len(y_true.columns)):
        similarity = jaccard_score(y_true.iloc[:, j], y_pred.iloc[:, i], average='macro', sample_weight=sample_weight)
        jaccard[y_pred.columns[i]].append(similarity)
        cnt+=1
    if cnt%1000==0:
        print(cnt)
        # print(f"Jaccard similarity between {y_true.columns[j]} and {y_pred.columns[i]}: {similarity[1]}")
jaccard_df = pd.DataFrame(jaccard).T
jaccard_df.rename(columns={int(item['remap_id']):item['org_id'] for idx, item in composition.iterrows()}, inplace=True)
jaccard_df['track'] = use_track['track'].values
jaccard_df['prediction'] = composition.iloc[np.argmax(jaccard_df.iloc[:,:-1], axis=1)]['org_id'].values
jaccard_df['jaccard_score'] = np.max(jaccard_df.iloc[:,:-2], axis=1).values
jaccard_df['label'] = np.argmax(jaccard_df.iloc[:,:-3],axis=1)
with open('track_id_name_list_processed_sample_weighted.txt', 'w') as f:
    f.write(f"org_id org_track remap_id remap_track\n")
    for idx, item in jaccard_df.iterrows():
        f.write(f"{idx}|{item['track']}|{item['label']}|{item['prediction']}\n")
