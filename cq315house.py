import requests
import json
import pymongo
import time
import re
from datetime import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import logging


def count_time_args(msg=None):
    def count_time(func):
        def wrapper(*args, **kwargs):
            t1 = time.time()
            results = func(*args, **kwargs)
            logging.info('%s 任务耗时：%s', msg, (time.time() - t1))
            print(f"{msg} 任务耗时：", time.time() - t1)
            return results

        return wrapper

    return count_time

@count_time_args(msg='spider_basic')
def spider_basic():
    """爬取一手房的基本信息，并将相关信息输入进mongo数据库，数据库集合命名用日期加上相关英文"""
    url = 'https://www.cq315house.com/WebService/WebFormService.aspx/getParamDatas'
    sitelist = ['44', '14', '39', '4', '9', '29', '19', '24', '34', '1149', '69']
    for i in sitelist:
        for j in range(1, 10000, 10):
            code_json = requests.post(url, json={
                "siteid": i,  # 渝中44 南岸14 江北39 渝北4 北碚9 大渡口29 沙坪坝19 九龙坡24 巴南34 两江新区1149 高新区69
                "useType": "",
                "areaType": "",
                "projectname": "",
                "entName": "",
                "location": "",
                "minrow": str(j),
                "maxrow": str(j + 10)
            }, verify=False).content.decode('utf-8')
            code_json = re.sub(r'\\', '', code_json)
            code_json = re.sub(r'\"\[', '[', code_json)
            code_json = re.sub(r']\"}', ']}', code_json)
            code_dict = json.loads(code_json)
            data = code_dict['d']
            if data==[]:
                break
            else:
                col_name = "cq315basic" + date_today
                collection = mongodb[col_name]
                collection.insert_many(data)


@count_time_args(msg='basic_transform')
def basic_transform():
    """将basic数据进行变形，同时将此次爬取的表格数据返回。"""
    transform_data = pd.DataFrame()
    col_name = "cq315basic" + date_today
    collection = mongodb[col_name]
    mongo_cq315basic = [x for x in collection.find({}, {'_id': 0})]
    mongo_cq315basicdf = pd.DataFrame(mongo_cq315basic)
    buildingid = mongo_cq315basicdf['buildingid']
    for i in range(len(buildingid)):
        data = pd.DataFrame()
        if len(buildingid[i].split(','))==1:
            data = mongo_cq315basicdf.iloc[[i]]
        else:
            data['blockname'] = mongo_cq315basicdf['blockname'][i].split(',')
            data['buildingid'] = mongo_cq315basicdf['buildingid'][i].split(',')
            data['counts'] = mongo_cq315basicdf['counts'][i]
            data['enterprisename'] = mongo_cq315basicdf['enterprisename'][i]
            data['f_presale_cert'] = mongo_cq315basicdf['f_presale_cert'][i]
            data['location'] = mongo_cq315basicdf['location'][i]
            data['projectid'] = mongo_cq315basicdf['projectid'][i]
            data['projectname'] = mongo_cq315basicdf['projectname'][i]
        transform_data = pd.concat([transform_data, data])
    transform_data['year'] = transform_data['f_presale_cert'].str.extract(r'(\d\d\d\d)', expand=True)
    with pd.ExcelWriter('cq315basic.xlsx') as writer:
        transform_data.to_excel(writer, index=False)
    return transform_data


@count_time_args(msg='spider_room')
def spider_room(search_df):
    """根据buildingid爬取各个房间号的明细数据，并同时将id和tag关联输入sql数据库"""
    url = 'https://www.cq315house.com/WebService/WebFormService.aspx/GetRoomJson'
    sql = 'INSERT INTO cq315tag(id, tag) VALUES (%s, %s)'
    buildingidset = set(search_df['buildingid'])
    while len(buildingidset)!=0:
        choose_id = buildingidset.pop()
        code_json = requests.post(url, json={
            "buildingid": choose_id
        }, verify=False).content.decode()
        code_json = re.sub(r'\\', '', code_json)
        code_json = re.sub(r'\"\[', '[', code_json)
        code_json = re.sub(r']\"}', ']}', code_json)
        try:
            code_dict = json.loads(code_json)
            choose_dict = code_dict['d']
            tag = code_dict['d'][0]['rooms'][0]['tag']
            if tag==[]:
                pass
            else:
                cur.execute(sql, (choose_id, tag))
                mysqldb.commit()
            for j in range(len(choose_dict)):
                data = choose_dict[j]['rooms']
                if data==[]:
                    pass
                else:
                    col_name = "cq315room" + date_today
                    collection = mongodb[col_name]
                    collection.insert_many(data)
            print(str(len(buildingidset)) + " " + choose_id + " " + "OK")
        except:
            print(str(len(buildingidset)) + " " + choose_id + " " + "有误")


@count_time_args(msg='export_cq315basic')
def export_cq315basic():
    """将basic数据汇总去重并重新装入mysql"""
    sql = 'SELECT * FROM cq315basic'
    cur.execute(sql)
    mysql_cq315basic = cur.fetchall()
    cq315basic = pd.DataFrame(mysql_cq315basic)
    cq315basic_new = pd.read_excel('cq315basic.xlsx', dtype='str')
    # cq315basic = cq315basic.append(cq315basic_new)
    cq315basic = pd.concat([cq315basic,cq315basic_new])
    cq315basic.drop_duplicates(subset=['projectid', 'buildingid', 'f_presale_cert'], keep='last', inplace=True)
    with pd.ExcelWriter('cq315basic.xlsx') as writer:
        cq315basic.to_excel(writer, index=False)
    sql = 'truncate cq315basic'
    cur.execute(sql)
    mysqldb.commit()
    cq315basic.to_sql(name='cq315basic', con=engine, if_exists='append', index=False, chunksize=5000)


@count_time_args(msg='export_cq315tag')
def export_cq315tag():
    """将cq315tag去重并重新输出"""
    sql = 'SELECT * FROM cq315tag'
    cur.execute(sql)
    mysql_cq315tag = cur.fetchall()
    cq315tag = pd.DataFrame(mysql_cq315tag)
    cq315tag.drop_duplicates(inplace=True)
    with pd.ExcelWriter('cq315tag.xlsx') as writer:
        cq315tag.to_excel(writer, index=False)
    sql = 'truncate cq315tag'
    cur.execute(sql)
    mysqldb.commit()
    cq315tag.to_sql(name='cq315tag', con=engine, if_exists='append', index=False, chunksize=5000)


@count_time_args(msg='export_cq315room')
def export_cq315room():
    """将room数据去重并装入sql数据库"""
    start_time = time.time()
    sql = 'SELECT * FROM cq315room'
    cur.execute(sql)
    mysql_cq315room = cur.fetchall()
    cq315room = pd.DataFrame(mysql_cq315room)
    end_time = time.time()
    total_time = end_time - start_time
    print('export_cq315room 1 任务耗时：' + str(total_time))

    start_time = time.time()
    mongodict = {'_id': 0}
    for column in cq315room:
        mongodict[column] = 1

    col_name = "cq315room" + date_today
    collection = mongodb[col_name]
    mongo_cq315room = [x for x in collection.find({}, mongodict)]
    mongo_cq315roomdf = pd.DataFrame(mongo_cq315room)
    end_time = time.time()
    total_time = end_time - start_time
    print('export_cq315room 2 任务耗时：' + str(total_time))

    start_time = time.time()
    cq315room = cq315room.append(mongo_cq315roomdf)
    # cq315room = pd.concat([export_cq315room, mongo_cq315roomdf])
    cq315room.drop_duplicates(subset=['id'], keep='last', inplace=True)
    end_time = time.time()
    total_time = end_time - start_time
    print('export_cq315room 3 任务耗时：' + str(total_time))

    start_time = time.time()
    cq315room.to_csv('cq315room.csv', index=False)
    sql = 'truncate cq315room'
    cur.execute(sql)
    mysqldb.commit()
    end_time = time.time()
    total_time = end_time - start_time
    print('export_cq315room 4 任务耗时：' + str(total_time))

    start_time = time.time()
    cq315room.to_sql(name='cq315room', con=engine, if_exists='append', index=False, chunksize=5000)
    end_time = time.time()
    total_time = end_time - start_time
    print('export_cq315room 5 任务耗时：' + str(total_time))


if __name__=='__main__':
    logging.basicConfig(
        level=logging.INFO,
        stream=open('logging.log', 'a'),
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    connection = pymongo.MongoClient()
    mongodb = connection.cq315
    date_today = datetime.now().strftime("%Y%m%d")
    mysqldb = pymysql.connect(host='localhost', port=3306, user='root', password='Xixu20940', database='cq315',
                              charset='utf8')
    cur = mysqldb.cursor(pymysql.cursors.DictCursor)
    engine = create_engine('mysql+pymysql://root:Xixu20940@localhost:3306/cq315')
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36"}

    spider_basic()
    search_df = basic_transform()
    spider_room(search_df)
    export_cq315basic()
    export_cq315tag()
    export_cq315room()

    cur.close()
    mysqldb.close()
