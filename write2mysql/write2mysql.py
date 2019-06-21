# coding:utf-8
import pymysql
import time
import os
import warnings
warnings.filterwarnings('ignore')
import argparse

def write2mysql(filepath):
    if not os.path.exists(filepath):
        print(filepath, 'not exists,please set the filepath')
    print('filepath: ',filepath)

    dataSet = []
    if filepath.endswith('过去一周单品销量.txt'):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                # print(line.split())
                if '餐盒费' in f:
                    continue
                item_id = line.split('|')[2]
                store_id = line.split('|')[0]
                if len(line.split('|'))==6:   ##正常
                    sales = line.split('|')[4]
                if len(line.split('|'))==5: ##少了item_name
                    sales = line.split('|')[3]
                # else:                      ##item_name之间有空格
                #     sales = line.split()[5]
                created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                updated_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                dataSet.append([item_id, store_id, sales, created_at, updated_at])

        sql1 = 'truncate table weekly_item_sales'
        sql2 = "INSERT INTO weekly_item_sales(item_id , \
                   store_id, sales ,created_at,updated_at) \
                   VALUES (%s,%s,%s,%s,%s)"

    elif filepath.endswith('过去一周套餐销量.txt'):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                package_id = line.split('|')[1]
                store_id = line.split('|')[0]
                sales = line.split('|')[2]
                created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                updated_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                dataSet.append([package_id, store_id, sales, created_at, updated_at])

        sql1 = 'truncate table weekly_package_sales'
        sql2 = "INSERT INTO weekly_package_sales(package_id , \
                   store_id, sales ,created_at,updated_at) \
                   VALUES (%s,%s,%s,%s,%s)"

    dataSet=dataSet[1:] ##去掉第一行字段行
    print(dataSet[:2])
    # 打开数据库连接
    db = pymysql.connect("10.88.67.251",
                         "db_user",
                         "123456",
                         "recommend_system"
                        )

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql1) ##清空表
        cursor.executemany(sql2, dataSet) ##插入数据
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()
        print('failed')
    # 关闭数据库连接
    db.close()
    print('finish')

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', help='file directory', type=str, required=True)
    args = parser.parse_args()
    # print('....', args.dir)
    write2mysql(args.dir)
