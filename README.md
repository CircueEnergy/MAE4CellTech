# automatic\_battery\_mask\_coding\_detector
利用MAE方式实现电池数据大模型预训练 

我们基于MAE的预训练模型，使用清华的四组实验数据，包含4个标签，做一个finetune的有监督学习。本代码库为包含推理在内的全部代码。

## 1.标签定义方式
0 代表正常

1 是实验二的异常标签，为没有rel的标记的电池片段

2 是实验三的异常标签，定义为平均库伦效率低于0.9的电池

3 是实验四的异常标签，以及轻微撕裂也就是2正1负这个标记的电池

核心代码为

```python
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
```

## 数据集划分

dataset\_split文件夹下面的代码，将数据集划分成随机的两份，一份用于训练一份测试

XX_charge_label.csv 是生成的片段标签元信息

XX_label2.csv 是电池单体的元信息

分为训练集和测试集，可以随机生成多份作交叉验证




## 实验结果
在当前项目的log文件夹


滑窗的结果

聚合到电池的结果

重新划分数据集，数据集3，库伦效率0.9划定标签，数据集4按照撕裂的程度划定标签，数据集2按照原始标签

```
# MAE
elect only columns which should be valid for the function.                           
  res = df.groupby('vin').mean().reset_index()                                     
train acc: 39 / 46 ,84.78260869565217%                                                                                                  
Test set: Average loss: 0.0094, Accuracy: 37919/43034 (88%)                                                                                       
test  acc: 23 / 34 ,67.64705882352942%                                                                                  
Test set: Average loss: 0.0357, Accuracy: 5386/9654 (56%) 
```




## 注意事项
第一次运行代码会在目录生成一个meta\_inf0\_XXXX.pkl 的二进制文件，这是数据预处理形成的索引文件，第一次会生成，会有一点慢，第二次运行就不会重新计算索引，直接从硬盘load加载。



## inference
```bash
python main_inference.py 
  --desc mae_inference \  # 取一个名字
  --finetune ../model/model.pth \ #model路径
  --model=battery_mae \  #哪一个模型
  --eval   # 指定是推理
```