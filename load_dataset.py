from itertools import islice

import pymongo
import re
import pandas as pd
import json


# 1.连接本地数据库服务
connection = pymongo.MongoClient('localhost')
# 2.连接本地数据库 demo。没有会创建
db = connection.sohu



# 3.创建集合
## rec_entity
rec_entity = db.rec_entity
rec_entity = []
for line in open('rec_data/recommend_content_entity_0317.txt','r',encoding='utf-8'):
    #分割数据
    line_json = json.loads(line)
    # print({'id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})
    #添加数据
    rec_entity.append({'_id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})

db['rec_entity'].drop()
db['rec_entity'].insert_many(rec_entity)

## nlp_train
nlp_train = db.nlp_train
nlp_train = []
for line in open('nlp_data/train.txt','r',encoding='utf-8'):
    #分割数据
    line_json = json.loads(line)
    # print({'id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})
    #添加数据
    nlp_train.append({'_id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})

db['nlp_train'].drop()
db['nlp_train'].insert_many(nlp_train)

## nlp_test
nlp_test = db.nlp_test
nlp_test = []
for line in open('nlp_data/test.txt','r',encoding='utf-8'):
    #分割数据
    line_json = json.loads(line)
    # print({'id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})
    #添加数据
    nlp_test.append({'_id': line_json['id'], 'content': line_json['content'], 'entity': line_json['entity']})

db['nlp_test'].drop()
db['nlp_test'].insert_many(nlp_test)

## submission1
submission1 = db.submission1
submission1 = []
with open('submission/section1.txt', 'r', encoding='utf-8') as f:
    f_after1 = f.readlines()[1:]
    f_after1 = [re.split('([{+])',x) for x in f_after1]
    #分割数据
    for line in f_after1:
        #添加数据
        submission1.append({'_id': line[0].strip(), 'result': line[1] + line[2]})

db['submission1'].drop()
db['submission1'].insert_many(submission1)


## submission2
submission2 = db.submission2
submission2 = []
with open('submission/section2.txt', 'r', encoding='utf-8') as f:
    f_after1 = f.readlines()[1:]
    f_after1 = [x.split("	") for x in f_after1]
    #分割数据
    for line in f_after1:
        #添加数据
        submission2.append({'_id': line[0].strip(), 'result': line[1]})

db['submission2'].drop()
db['submission2'].insert_many(submission2)