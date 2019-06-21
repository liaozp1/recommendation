# -*- coding: utf-8 -*-
import json
import os
import time
import shutil

def file_run(fileroot, filelist):
    for filename in filelist:
        file_name_sql = os.path.join(fileroot + filename)
        file_name_csv = os.path.join(os.path.splitext(file_name_sql)[0]+'.csv')
        file_name_top = os.path.join(os.path.splitext(file_name_sql)[0] + '.top')

        f_sql = open(file_name_sql, 'r',encoding="utf-8")
        f_csv = open(file_name_csv, 'w',encoding="utf-8")
        f_top = open(file_name_top, 'w',encoding="utf-8")

        n = 10
        for line in f_sql:
            lx = line.split("),(")
            for stem in lx:
                wl = stem[stem.find('{'):stem.rfind('}') + 1]
                if len(wl) >= 10:
                    n = n - 1
                    if n >= 0:
                        f_top.write(wl + "\n")
                    f_csv.write(wl + "\n")

        f_sql.close()
        f_csv.close()
        f_top.close()

def readfile_to_ItemList(fileroot,filelist):
    file_out = os.path.join(fileroot, 'goods.Item')
    print(file_out)
    f_out = open(file_out, 'w', encoding="utf-8")
    for filename in filelist:
        file_in = os.path.join(fileroot + filename)
        # file_out = os.path.join(os.path.splitext(file_in)[0] + '.ItemList')
        print(file_in)
        # print(file_out)

        f_in = open(file_in, 'r', encoding="utf-8")
        # f_out = open(file_out, 'w',encoding="utf-8")
        for line in f_in:
            new_dict = json.loads(line.replace("\\", ""))
            items=[]
            for stem in new_dict["ItemList"]:
                # print(stem)
                if '包装餐具' in stem['Name'] or '外送费' in stem['Name'] or '尊享缴费' in stem['Name']\
                        or '尊享续费' in stem['Name'] or '餐盒费' in stem['Name'] or '尊享卡缴费' in \
                        stem['Name']:
                    continue
                items.append(stem['Name'])
            if len(items)==0:
                continue
            items_str = ' '.join(items)+'\n'
            # print(items_str)
            f_out.write(items_str)
        f_in.close()
    f_out.close()
    print("finish")

def split2file(fileroot):
        file_in = os.path.join(fileroot, 'tuijian_data.txt')
        with open(file_in, 'r') as f1:
            for line in f1:
                # print('lineraw:',line)
                line = ' '.join(line.strip().replace('|', '').split())
                # print('line:', line)
                store_name=line.strip().split()[0]
                file_store=os.path.join(fileroot, store_name +'.txt')
                # print(file_store)
                try:
                    with open(file_store, 'a') as f2:
                        f2.write((' '.join(line.split()[2:])+'\n'))
                except:
                    pass
                n = n+1
                if n % 5000000==0:
                    print('.......step %s*500 .......'%(n//5000000))
        print('.................finish....................')


def split_files(fileroot):
    try:
        shutil.rmtree(fileroot + 'stores') #每次执行前删除stores文件夹
    except:
        print('no such file:%s'%(fileroot + 'stores'))
    ##在当前目录下创建stores文件夹
    if not os.path.exists(fileroot + 'stores'):
        os.makedirs(fileroot + 'stores')
    filenames = [file for file in os.listdir(fileroot) if file.endswith('txt')] ##多个文件
    print(filenames)
    dic = {} ##key：店铺路径 value: 订单内容
    n = 0 ##记录写入条数
    for filename in filenames:
        file_in = os.path.join(fileroot, filename)
        # print(file_in)
        with open(file_in, 'r') as f1:
            for line in f1:
                # print(line)
                ##替换竖线分隔符
                line = ' '.join(line.strip().replace('|', '').split())
                # print('line:', line)
                store_name = line.strip().split()[0]
                file_store = os.path.join(fileroot, 'stores/' + store_name + '.txt')
                content = ' '.join(line.split()[2:]) + '\n'
                if file_store not in dic:
                    dic[file_store] = [content]
                else:
                    dic[file_store].append(content)
                if len(dic[file_store])==2000:  ##每隔1000条写一次
                    try:
                        with open(file_store, 'a') as f2:
                            f2.writelines(dic[file_store])
                            # print('write to %s'%(file_store))
                            n=n+2000
                            if n%1000000 == 0:
                                print('写入%s百万条数据'%(n//1000000))
                        dic[file_store]=[]   ##写完即清空
                    except:
                        pass
    for file_store in dic:
        if len(dic[file_store])!=0:
            try:
                with open(file_store, 'a') as f3:
                    f3.writelines(dic[file_store])
            except:
                pass
    print('.................finish....................')

if __name__ == '__main__':
    start = time.time()
    fileroot = 'D:/work/FPgrowth-master/FPgrowth-master/data/orders/longterm/'
    # 将数据从原始sql文件中解析出来
    # file_sql=[file for file in os.listdir(fileroot) if file.endswith('sql')]
    # file_run(fileroot, file_sql)

    # 将数据处理成ItemList，用户关联分析
    # file_csv = [file for file in os.listdir(fileroot) if file.endswith('csv')]
    # readfile_to_ItemList(fileroot, file_csv)
    # split2file(fileroot)
    split_files(fileroot)
    print(time.time() - start, 'sec')
