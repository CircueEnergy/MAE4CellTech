import copy
import os
import pickle
import pandas as pd
from loguru import logger
import json
import tqdm_pathos
import random
from preprocess.datasets.normalizer import Normalizer2
from preprocess.pre_utils import sampling

random.seed(0)
from datetime import datetime
from collections import OrderedDict


class BaseBatteryData:
    """
           初始化
           :param data_path: 数据路径 str
           :param patch_len: patch长度 int
           :param tokens_len: 序列长度 int
           :param interpolate: 数据最小间隔 int
           :param jobs: 进程数 int
           :param ram: 是否开启内存 bool
    """

    def __init__(self, battery_data_args):
        """
        初始化
        :param battery_data_args: dict，包含是否常驻内存，插值，分块个数，单个滑窗大小
         { list<str>  数据路径          "data_path": args.data_path,
           path_dir  norm的父路径        "norm_path": args.norm_path,
           int patch的长度             "patch_len": args.patch_len,
           str all有单体，小满的桩端随意       car_type": args.car_type,
           bool   是否常驻内存           "ram": args.ram,
           int 每个滑窗长度，计算索引用             "single_data_len": args.patch_len * args.tokens_len,
           int   插值的间隔             "interpolate": args.interpolate,
           int 多少进程用于多进程读取数据，一般设置为cpu核心数             "jobs": args.num_workers,
           bool 是否分块读数据              "partition_train": args.partition_train,
           int 分多少块读以及训练            "partition_nums": args.partition_nums
        }
        """
        self.df_lst = {}  # 文件的个数
        self.data_lst = []
        self.battery_dataset = []
        self.p_datapath = []
        self.jobs = battery_data_args["jobs"]
        self.ram = battery_data_args["ram"]
        self.car_type = battery_data_args["car_type"]
        self.data_path = battery_data_args["data_path"]
        self.norm_path = battery_data_args["norm_path"]
        self.interpolate = battery_data_args["interpolate"]
        self.single_data_len = battery_data_args["single_data_len"]
        self.partition_train = battery_data_args["partition_train"]
        self.partition_nums = battery_data_args["partition_nums"]
        self.label_path = battery_data_args["label_path"]
        self.check_dataset_files()
        if self.partition_train:
            logger.info("使用partition加载数据")
            self.part_len = len(self.data_lst) // self.partition_nums

        if self.car_type == 'all':
            self.train_columns = ['current', 'soc', 'max_single_volt', 'min_single_volt', 'max_temp', 'min_temp', ] + [
                'mean_volt', 'std_volt', 'std_temp', 'mean_temp']
        elif self.car_type == 'xiaoman':
            self.train_columns = ['volt', 'current', 'soc', 'max_single_volt', 'min_single_volt', 'max_temp',
                                  'min_temp', ]
        else:
            self.train_columns = ['volt', 'current']

        self.check_normalize()
        self.meta_info()

    def meta_info(self):
        """
        创建索引文件
        :return:
        """
        if not os.path.exists(f"meta2_{self.car_type}_{self.single_data_len}_{self.label_path.split('/')[-1]}.pkl"):
            logger.info("没有缓存索引文件，开始从零开始创建")
            self.results = tqdm_pathos.map(pool_map, [[self, file] for file in self.data_lst], n_cpus=self.jobs)
            random.shuffle(self.results)
            with open(f"meta2_{self.car_type}_{self.single_data_len}_{self.label_path.split('/')[-1]}.pkl",'wb') as f:
                pickle.dump(self.results, f)
        else:
            logger.info("已经有了缓存索引文件，开始加载索引")
            with open(f"meta2_{self.car_type}_{self.single_data_len}_{self.label_path.split('/')[-1]}.pkl", 'rb') as f:
                self.results = pickle.load(f)

    def get_part_data(self, part_num):
        logger.info(f"开始加载第{part_num}段数据")
        self.battery_dataset = []
        self.df_lst = {}
        logger.info(f"所有的片段长度为 {len(self.data_lst)}")
        self.mutilprocess_read(part_num=part_num)

    def get_all_data(self):
        logger.info(f"开始加载全部数据")
        self.mutilprocess_read(part_num=0)

    def check_dataset_files(self):
        # 获取文件列表，检测是否有数据文件
        """
        获取需要训练的片段数据路径
        :return:
        """
        df_label = pd.read_csv(self.label_path)
        self.data_lst = [file for file in list(df_label['charges'])]
        logger.info(f'长度：{len(self.data_lst)}')
        if len(self.data_lst) == 0:
            logger.error('.csv or .feather file not found')
            exit()

    def mutilprocess_read(self, part_num):
        """
        多进程读取数据
        """
        logger.info(f'一共有{len(self.results)}个滑窗,前10个滑窗是{self.results[:10]}')
        if part_num > 0:
            logger.info(f'分块练，这是第{part_num}块数据')
            results_ = self.results[(part_num - 1) * self.part_len:part_num * self.part_len]
        else:
            results_ = copy.deepcopy(self.results)

        if self.ram:
            for file, df in [return_list_tuple[1] for return_list_tuple in results_]:
                self.df_lst[file] = df
        for single_file_tuple in [return_list_tuple[0] for return_list_tuple in results_]:
            self.battery_dataset.extend(single_file_tuple)
        logger.info("data length %d" % len(self.battery_dataset))

    def check_normalize(self):
        """
        加载norm文件
        :return:
        """
        self.normalizer = Normalizer2(params={
            'mean': [3.735813521060345, -23.526959904250266],
            'std': [0.288631523438454, 26.577791042819808]
        }
        )

    def read_file(self, file, cell_is_list=False):
        """
        读取csv或者feather的代码
        :param file:
        :return:
        """
        df = None
        if file.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.endswith('.feather'):
            df = pd.read_feather(file)
        if 'exp_2' in file:
            df.rename(columns={'V': 'volt', 'I': 'current', 'T': 'timestamp'}, inplace=True)
            df['timestamp'] = df['timestamp'].map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
            df['timestamp'] = df['timestamp'].map(lambda x: int(x.value / 10e8))
        if 'exp_3' in file:
            pass
        if 'exp_4' in file:
            pass
        retain_columns = ['volt', 'current', 'timestamp']
        # 串？并？怎么平衡单体电流
        df = df[retain_columns]

        return df

    def columns_split(self, dataframe, column_list_name, column_name):
        """ 拆分特定单列数据为多列数据

        Args:
            dataframe：dataframe格式的数据集
            column_list_name：需要拆分的特定单列数据名称
            column_name：拆分之后多列数据名称的前缀，例如：volt，temp等

        Returns：
            new_dataframe：所需维度名称标准化后的dataframe数据集
        """
        new_columns = [column_name + str(i + 1) for i in range(len(dataframe[column_list_name][0].split(";")))]
        dataframe[new_columns] = dataframe[column_list_name].str.split(';', expand=True)
        dataframe[new_columns] = dataframe[new_columns].astype(float)
        dataframe.drop(columns=[column_list_name], inplace=True)  # 删除需要拆分的特定单列数据名称
        return dataframe

    def __len__(self):
        """
        获取数据集长度
        """
        return len(self.battery_dataset)

    def __getitem__(self, index):
        """
        起到迭代器的作用，通过索引读取数据
        Args:
            index: 0，1，2......
        Returns:
            返回数据：df
        """
        file, time0, metadata = self.battery_dataset[index]
        if self.ram:
            df = self.df_lst[file]
        else:
            df = self.read_file(file)
            if self.interpolate:
                df = sampling(df, self.interpolate)
        df = df[self.train_columns]
        df = df.iloc[time0: time0 + self.single_data_len]
        df = df.values
        # 直接输出归一化的数据
        df = self.normalizer.norm(df)
        return df, metadata


def pool_map(args):
    """
    多进程读取数据
    file 要读取的文件名
    """
    self, file = args[0], args[1]

    return_lst = []
    metadata = OrderedDict()
    name = os.path.splitext(file.split('/')[-1])[0]
    #     metadata['vin'] = name[2] + "_" + name[3]
    # metadata['label'] = 0 if os.path.splitext(file)[0].split('_')[0] == '00' and  os.path.splitext(file)[0].split('_')[1] == '00' else 1
    metadata['label'] = get_label(file)
    metadata['vin'] = get_vin(file)

    df = self.read_file(file)  # 读取数据
    if self.interpolate:  # 对df进行插值处理
        df = sampling(df, self.interpolate)
    if df.shape[0] >= self.single_data_len:
        sequence_num = df.values.shape[0] // self.single_data_len
        for index in range(sequence_num):
            return_lst.append((file, index * self.single_data_len, copy.deepcopy(metadata)))
    if self.ram:
        return [return_lst, (file, df,)]
    else:
        return [return_lst, ()]


def get_label(file):
    # 根据文件名获取label
    if 'exp_2' in file:
        file = os.path.splitext(file.split('/')[-1])[0]
        names = file.split('-')
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
            return 0
        return 3

def get_vin(file):
    file_ = os.path.splitext(file.split('/')[-1])[0]
    names = file_.split('-')
    print(names)
    if 'exp_2' in file:
        if 'ref' == names[0]:
            return f'exp2_{names[0]}_{names[1]}'
        else:
            return f'exp2_{names[0]}_{names[1]}'

    if 'exp_3' in file:
   
        return f'exp3_{names[0]}_{names[1]}_{names[2].split("_")[0]}'
    if 'exp_4' in file:
        names = os.path.splitext(file.split('/')[-1])[0].split("_")
        return f'exp4_{names[0]}_{names[1]}#'
