#!/usr/bin/env python3
# encoding: utf-8


import os
import random

import pandas as pd
from loguru import logger
from tqdm import tqdm


def get_label(file):
    # 根据文件名获取label
    if 'exp_2' in file:
        file = os.path.splitext(file.split('/')[-1])[0]
        names = file.split('-')
        #     print(names)
        if names[0] != 'ref':
            return 1
        else:
            return 0
    if 'exp_3' in file:
        labels = [{'exp3_CE01_SS200正_1#': 0.9053031314226511},
         {'exp3_CE01_SS200正_5#': 0.9035441499465301},
         {'exp3_CE01_Cu100正_3#': 0.7522478559459914},
         {'exp3_CE01_SS100负_4#': 0.8637556348647137},
         {'exp3_CE01_SS200正_2#': 0.8761735072917751},
         {'exp3_CE01_SS100负_2#': 0.8886775437291056},
         {'exp3_CE01_Cu100正_5#': 0.9184458601446746},
         {'exp3_CE01_SS100正_4#': 0.9052794766043538},
         {'exp3_CE01_Cu100正_6#': 0.8871285195993326},
         {'exp3_CE01_PMMA200_1#': 0.8838408871899787},
         {'exp3_CE01_铜焊渣_1#': 0.901298268881915},
         {'exp3_CE01_PMMA200_5#': 0.8884381236519269},
         {'exp3_CE01_SS200负_3#': 0.8851141495533793},
         {'exp3_CE01_SS100负_5#': 0.9013709760475954},
         {'exp3_CE01_铝焊渣_2#': 0.9021448629509822},
         {'exp3_CE01_SS100正_3#': 0.9002640065746463},
         {'exp3_CE01_SS100正_5#': 0.860053042419221},
         {'exp3_CE01_SS100负_1#': 0.90263758686548},
         {'exp3_CE01_SS200负_2#': 0.9134100300696172},
         {'exp3_CE01_铝焊渣_3#': 0.8973758728272817},
         {'exp3_CE01_SS200负_5#': 0.9057923566068055},
         {'exp3_CE01_PMMA200_7#': 0.860405558639989},
         {'exp3_CE01_PMMA200_6#': 0.9098116172501135}]
        labels_dict = {}
        for d in labels:
            for key,value in d.items():
                labels_dict[key] = value
        return 0 if labels_dict[get_vin(file)] > 0.9 else 2
    if 'exp_4' in file:
        if "Data1" in file or "Data3" in file:
            return 1
        return 3

def get_vin(file):
    file_ = os.path.splitext(file.split('/')[-1])[0]
#     print(file_)
    names = file_.split('-')
    print(names)
    if 'exp_2' in file:
        if 'ref' == names[0]:
            return f'exp2_{names[0]}_{names[1]}'
        else:
            return f'exp2_{names[0]}_{names[1]}'
    #     print(names)
#         if names[0] != 'ref':
#             return 1
#         else:
#             return 0
    if 'exp_3' in file:
        # print(names)
        # print(f'exp3_{names[2].split("_")[0]}')
        return f'exp3_{names[0]}_{names[1]}_{names[2].split("_")[0]}'
    if 'exp_4' in file:
        names = os.path.splitext(file.split('/')[-1])[0].split("_")
        # print(names)
        print(f'exp4_{names[0]}_{names[1]}#')
        return f'exp4_{names[0]}_{names[1]}#'


class TrainTest:

    def __init__(self, all_data_path):
        self.all_charge_path = []
        for path in all_data_path:
            files = os.listdir(path)
            for file in files:
                self.all_charge_path.append(os.path.join(path,file))


    def get_file_label(self):
        """ 获取文件名称中的标签和车辆标识，返回统计之后的正常标签车辆字典和异常标签车辆字典，字典信息包含vin和充电段总个数

        args:
            无

        return:
            normal_dict: 所有正常标签车辆字典，例如：{"aaa": 100, "bbb": 90}
            abnormal_dict：所有异常标签车辆字典，例如：{“ccc”: 50, "ddd": 40}
        """
        label_dict = {}

        for file_name in tqdm(self.all_charge_path):
            shot_name, extension = os.path.splitext(file_name)  # 00_aabb_1.csv
            label = get_label(file_name)#shot_name.split('_')[0]
            # car_name = shot_name.split('_')[1]
            car_name = get_vin(file_name)
            if label not in label_dict.keys():
                label_dict[label] = {}
                label_dict[label][car_name] = 1
            else:
                if car_name not in label_dict[label].keys():
                    label_dict[label][car_name] = 1
                else:
                    label_dict[label][car_name] += 1
        normal_dict = label_dict[0]
        abnormal_dict = label_dict[1]
        print(normal_dict)
        print(abnormal_dict)
        return normal_dict, abnormal_dict ,label_dict[2],label_dict[3]

    @staticmethod
    def split(deal_dict, more=6):
        """ 按照某比例占比划分数据，将数据分为两部分

        args:
            deal_dict：划分前存放所有数据的列表
            more：某比例占比，默认为6

        return:
            left_file: 随机选取的部分数据
            right_file：未随机选取的部分数据
        """
        length = len(deal_dict)
        print(length)
        vin_list = deal_dict.keys()
        i = 1
        while True:
            if i < 1000000:
                # 随机划分
                left_file = random.sample(vin_list, int(length * more / 10))
                left_charge_num = sum([deal_dict[vin] for vin in left_file])
                right_file = list(set(vin_list).difference(set(left_file)))
                right_charge_num = sum([deal_dict[vin] for vin in right_file])
                ratio = left_charge_num / right_charge_num

                if (ratio > 1.5) and (ratio < 2.33):  # 划分比例在6:4和7:3之间
                    logger.info(f"left file num is {len(left_file)}, right file num is {len(right_file)}, ratio is {len(left_file) / len(right_file)}")
                    logger.info(f"left charge num is {left_charge_num}，right charge num is {right_charge_num}，ratio is {ratio}")
                    return left_file, right_file
                else:
                    i += 1
                    continue
            else:
                # 循环次数>=100万次，则认为样本均衡失败，返回None
                return None, None

    @staticmethod
    def get_train_test_valid(normal_data_file, abnormal_data_file,abnormal_data_file2,abnormal_data_file3):
        """ 获取所有车辆（正常车和异常车）按照某比例分成的训练集和测试集

        args:
            normal_data_file：所有正常车辆字典
            abnormal_data_file：所有异常车辆字典

        return:
            level2_left: 按照某占比比例的正常车和某占比比例的异常车组成的训练集
            level2_right：按照某占比比例的正常车和某占比比例的异常车组成的测试集
        """
        # level1
        normal_level1_left, normal_level1_right = TrainTest.split(normal_data_file)
        left2,right2 =  TrainTest.split(abnormal_data_file)
        left3, right3 = TrainTest.split(abnormal_data_file2)
        left4, right4 = TrainTest.split(abnormal_data_file3)
        # if normal_level1_left is None and normal_level1_right is None:
        #     logger.info("循环次数过多，仍无预期结果")
        #     exit()
        # else:
        # abnormal_level1_left, abnormal_level1_right = TrainTest.split(abnormal_data_file)

        # level2
        level2_left = normal_level1_left + left2 + left3 + left4
        level2_right = normal_level1_right + right2 + right3 + right4

        return level2_left, level2_right

    def copy_train_test_data_set(self):
        """ 拷贝训练集和测试集数据到相应路径下

        args:
            无

        return:
            无
        """
        normal, abnormal,abnormal2,abnormal3 = self.get_file_label()
        train_charge, test_charge = TrainTest.get_train_test_valid(normal, abnormal,abnormal2,abnormal3)
        train_charges = [file for file in self.all_charge_path if  get_vin(file) in train_charge]
        test_charges = [file for file in self.all_charge_path if
                        get_vin(file) in test_charge]
        train_result = pd.DataFrame(columns=('car', 'label'))
        test_result = pd.DataFrame(columns=('car','label'))
        for file in tqdm(train_charges):
            name = get_vin(file) # ['00', 'LZ91AE2AXJ3LSA015', '438']
            label = get_label(file) # 标签
            car = name  # 车名
            train_result = train_result.append(pd.DataFrame({'car': [car], 'label': [label],'charges':[file]}), ignore_index=True)
        train_result.drop_duplicates().reset_index(drop=True).to_csv("train_charge_label.csv",  index=False)
        train_result[['car','label']].drop_duplicates().reset_index(drop=True).to_csv("train_label2.csv",  index=False)

        for file in tqdm(test_charges):
            # print(file)
            name =  get_vin(file)   # ['00', 'LZ91AE2AXJ3LSA015', '438']
            label =   get_label(file)  # 标签
            car = name  # 车名
            test_result = test_result.append(pd.DataFrame({'car': [car], 'label': [label],'charges':[file]}), ignore_index=True)
        test_result.drop_duplicates().reset_index(drop=True).to_csv("test_charge_label.csv",  index=False)
        test_result[['car', 'label']].drop_duplicates().reset_index(drop=True).to_csv("test_label2.csv", index=False)




if __name__ == "__main__":
    path = ['/data/nfsdata/database/exp/tsinghua_20230428/result/exp_2/data/',  '/data/nfsdata/database/exp/tsinghua_20230428/result/exp_3/data',
                                   '/data/nfsdata/database/exp/tsinghua_20230428/result/exp_4/data']
    train_test = TrainTest(all_data_path=path)
    train_test.copy_train_test_data_set()
