import pyspark
import pandas as pd
import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import time
import gc

start = time()

spark = SparkSession.builder.config("spark.driver.memory",'10g').appName("DataFrame").getOrCreate()
spark.catalog.clearCache()

lastfm_le = spark.read.format('csv').option("header", False).option("delimiter",'\t').load("/home/lafesta/Documents/Graduation_Project/data/LFM-1b_LEs.txt")
artists = spark.read.format('csv').option("header", False).option("delimiter", '\t').load("/home/lafesta/Documents/Graduation_Project/data/LFM-1b_artists.txt")
tracks = spark.read.format('csv').option("header", False).option("delimiter", '\t').load("/home/lafesta/Documents/Graduation_Project/data/LFM-1b_tracks.txt")
meta_df = pd.read_csv('data/musicnet_metadata.csv')


# df_list = []
# chunk_size=10**6
# column_names = ['user_id', 'artist_id', 'album_id', 'track_id']
# for chunk in pd.read_csv('data/LFM-1b_LEs.txt', sep='\t', chunksize=chunk_size, names=column_names):
#     df_list.append(chunk)

# lastfm_le = pd.concat(df_list)
# print(lastfm_le.show(3))
lastfm_le = lastfm_le.select(col("_c0").alias('user_id'), col("_c1").alias("artist_id"), col("_c2").alias('track_id'), col("_c3").alias('timestamp'))
# lastfm_le = lastfm_le.rename(columns={0:'user_id', 1:'artist_id', 2:'album_id', 3:'track_id', 4:'timestamp'})
artists = artists.select(col("_c0").alias('artist_id'), col("_c1").alias("artist_name"))
# tracks = tracks.rename(columns={0:'track_id', 1:'track_name', 2:'artist_id'})
tracks = tracks.select(col("_c0").alias('track_id'), col("_c1").alias("track_name"), col("_c2").alias("artist_id"))
with open('data/composers.json', 'r') as f:
        composers = json.load(f)
        
composers_df = pd.DataFrame(composers['composers'])
composers_df.rename(columns={'name':'composer'}, inplace=True)

composer_uniq = [item for item in composers_df.loc[composers_df['complete_name'].str.contains('|'.join(meta_df['composer'].unique()), na=False, case=False), 'complete_name']]

use_artist = artists.filter(artists.artist_name.isin(composer_uniq))
# print(use_artist.show(5))
# print(lastfm_le.head())
# print(use_artist.head())
unique_artist = use_artist.select("artist_id").distinct().rdd.flatMap(lambda x: x).collect()
use_le = lastfm_le.filter(lastfm_le.artist_id.isin(unique_artist))



gc.collect()

use_le.cache()

# print(len(use_le))
idx = 0
track_lst = []
track_dict = {}

track_uniq = list(meta_df['composition'].unique())
# print(unique_artist)
use_track_id = tracks.filter(col('artist_id').isin(unique_artist)).select("track_id").distinct().rdd.flatMap(lambda x: x).collect()

# unique_track = use_le.select("track_id").distinct().rdd.flatMap(lambda x: x).collect()
# unique_track_id = use_track_id.select("track_id").distinct().rdd.flatMap(lambda x: x).collect()
print(len(use_track_id))
for id in use_track_id:
    track_lst.append(f"{id} {idx}")
    track_dict[id] = idx
    idx+=1
# print(track_dict)
idx = 0
# user_lst = []
user_dict = {}
# print(use_track_id.filter)
unique_user_id = use_le.filter(col('track_id').isin(use_track_id)).select("user_id").distinct().rdd.flatMap(lambda x: x).collect()

print(len(unique_user_id))
for id in unique_user_id:
    # user_lst.append(f"{id} {idx}")
    user_dict[id] = idx
    idx+=1

train_lst = {}
cnt=0


for id in user_dict.keys():
    train_lst[user_dict[id]] = []
    for track in use_le.filter(use_le.user_id == id).select('track_id').rdd.flatMap(lambda x: x).collect():
        if track in use_track_id:
            train_lst[user_dict[id]].append(str(track_dict[track]))
    cnt+=1

    if cnt%1000==0:
        print(cnt)
print(len(train_lst))
with open('data/item_list.txt', 'w') as f:
    f.write("org_id remap_id\n")
    for k,v in track_dict.items():
        f.write(f"{k} {v}\n")

with open('data/user_list.txt', 'w') as f:
    f.write("org_id remap_id\n")
    for k,v in user_dict.items():
        f.write(f"{k} {v}\n")


train_len = int(len(train_lst) * 0.8)
test_len = int((len(train_lst) - train_len) * 0.5)
valid_len = len(train_lst) - train_len - test_len
items = list(train_lst.items())
train = dict(items[:train_len])
test = dict(items[train_len:train_len + test_len])
valid = dict(items[train_len+test_len:])

with open('data/train.txt', 'w') as f:
    idx = 0
    for k,v in train.items():
        val = ' '.join(v)
        f.write(f"{idx} {val}\n")
        idx+=1
with open('data/test.txt', 'w') as f:
    idx = 0
    for k,v in test.items():
        val = ' '.join(v)
        f.write(f"{idx} {val}\n")
        idx+=1
with open('data/valid.txt', 'w') as f:
    idx = 0
    for k,v in valid.items():
        val = ' '.join(v)
        f.write(f"{idx} {val}\n")
        idx+=1
end = time()
print("Total runtime: ", end-start)
