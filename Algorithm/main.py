# coding:utf-8
import fpgrowth_lzp as fpgrowth
import time
import redis
import json
import os
import argparse
import warnings
warnings.filterwarnings('ignore')
"""
'''simple data'''
simDat = fpgrowth.loadSimpDat()
initSet = fpgrowth.createInitSet(simDat)
myFPtree, myHeaderTab = fpgrowth.createFPtree(initSet, 3)
myFPtree.disp()

print(fpgrowth.findPrefixPath('z', myHeaderTab))
print(fpgrowth.findPrefixPath('r', myHeaderTab))
print(fpgrowth.findPrefixPath('t', myHeaderTab))

freqItems = []
fpgrowth.mineFPtree(myFPtree, myHeaderTab, 3, set([]), freqItems)
for x in freqItems:
    print(x)
"""
def transfer2int(r, dataSet):
    """
    :param r:
    :param dataSet:
    :return: dadaset:elmenets are numeber ids
    """
    dataSet_= list(set([y for x in dataSet for y in x]))
    items2ids = {v: k for k, v in enumerate(dataSet_)}
    ids2items = {k: v for k, v in enumerate(dataSet_)}

    r.set('items2ids', json.dumps(items2ids, ensure_ascii=False), nx=True)
    r.set('ids2items', json.dumps(ids2items, ensure_ascii=False), nx=True)
    items2ids = json.loads(r.get('items2ids'))
    dataset=[]
    for items in dataSet:
        items = list(map(lambda i: items2ids[i], items))
        dataset.append(items)
    return dataset


def gen_rules(filepath, args):
# def gen_rules(filepath):
    if not os.path.exists(filepath):
        print(filepath, 'not exists,please set the filepath')
    print('\n\n\n')
    print('------------------------------处理文件%s-----------------------------------'%(os.path.basename(filepath)))
    with open(filepath, encoding='utf-8') as f:
        dataSet = [line.split() for line in f.readlines()]
    # db = pymysql.connect("10.88.67.251", "db_user", "123456", "recommend_sys_config")
    # cursor = db.cursor()
    # ###以单个店为个体进行推荐
    # cursor.execute(
    # "SELECT a.Items FROM (SELECT Store_Id,New_Bill_Id, GROUP_CONCAT(Material_Name) AS Items FROM test GROUP BY "\
    # "New_Bill_Id ,Store_Id) a WHERE a.Store_Id=3011001")
    # dataSet= list(cursor.fetchall())
    # # 关闭数据库连接
    # db.close()
    # print(dataSet[:10])
    # dataSet=(list(map(lambda x: ''.join(x).replace('\r', '').split(','), dataSet)))
    # print('dataSet:', dataSet[:2])
    # dataSet = transfer2int(r, dataSet) ###将item映射为id
    if len(dataSet) <= 100: ##如果交易数据少于100条，返回空
        print('-----------------------交易数据小于100条，不生成rules-------------------------')
        return []
    frozenDataSet = fpgrowth.transfer2FrozenDataSet(dataSet)
    # print('frozenDataSet长度：', len(frozenDataSet))
    # print('frozenDataSet：', frozenDataSet)
    ###构建树
    support_num = args.support*len(dataSet)  ###支持数=支持度*交易数目
    # support_num = 20000
    fptree, headPointTable = fpgrowth.createFPTree(frozenDataSet, support_num)
    ###挖掘树
    frequentPatterns = {}
    prefix = set([])
    fpgrowth.mineFPTree(headPointTable, prefix, frequentPatterns, support_num)
    # print("frequent patterns:")
    # print(frequentPatterns)
    # print(len(frequentPatterns))
    rules = []
    fpgrowth.rulesGenerator(frequentPatterns, args.confidence, rules)
    # fpgrowth.rulesGenerator(frequentPatterns, 0.7, rules)
    #取后件为1的rules
    print(rules)
    filter_rules = [rule for rule in rules if len(rule[1]) == 1]
    filter_rules = sorted(filter_rules, key=lambda p: p[2], reverse=True)
    print('number of association rules:\n', len(filter_rules))
    return filter_rules

def write2redis(r, rules,filepath):
    if args.type == 'long':
        pre = 'LAR:'+ os.path.basename(filepath).replace('.txt', '')
        pre_n = 'LRN:'+ os.path.basename(filepath).replace('.txt', '')
    else:
        pre = 'SAR:'+ os.path.basename(filepath).replace('.txt', '')
        pre_n = 'SRN:'+ os.path.basename(filepath).replace('.txt', '')
    # print(args.type + " association rules:")
    if len(rules) > 0:
        for rule in rules:
            # print(pre + ''.join(list(map(str, sorted(rule[0])))), '————————>>', ' '.join(list(map(str,\
            #         sorted(rule[1])))), ', confidence:', rule[2], 'frequentNums:', rule[3], 'subsetNums:', rule[4])
            name = pre+''.join(list(map(str, sorted(rule[0]))))
            key = ''.join(list(map(str, sorted(rule[1]))))
            value = rule[2]
            mapping={key: value}
            ##name为按照数字顺序排列的 例如(3,2,1)为'123'
            # r.zadd(pre + ''.join(list(map(str, sorted(rule[0])))), mapping, ch=True) ##sortedset存储规则
            r.zadd(name, mapping, ch=True)
        r.set(pre_n, len(rules)) ##string存储规则数目
        print('----------------------%s规则写入redis完毕-----------------------'%(os.path.basename(filepath)))
        # print(len(r.keys()))
        # print(r.zrevrange('SAR:111', 0, -1, withscores=True)) ##降序排序
        # print('jsonloads: ', json.loads(r.get('ids2items')))
    else:
        pass

if __name__=='__main__':
    start = time.time()
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--type', help='gennerate LAR or SAR', type=str, default='long', required=True)
    # parser.add_argument('--dir', help='file directory', type=str, default= \
    #                     "D:/work/FPgrowth-master/FPgrowth-master/data/orders/longterm/stores/", required=True)
    # parser.add_argument('-s', '--support', help='support', type=float, default=0.1, required=True)
    # parser.add_argument('-c', '--confidence', help='confidence', type=float, default=0.7, required=True)
    # args = parser.parse_args()
    # # print(args)
    # # host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
    # pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    # r = redis.Redis(connection_pool=pool)
    #
    # store_files = [file for file in os.listdir(args.dir)]  ##多个文件
    # for filename in store_files:
    #     store_file = os.path.join(args.dir, filename)
    #     # print('store_file:', store_file)
    #     rules = gen_rules(store_file, args)
    #     write2redis(r, rules, store_file)
    filepath='./fpgrowth/data/kosarak.dat'
    rules=gen_rules(filepath)
    print(time.time() - start, 'sec')