import traceback
import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pytorch.basic.utils import getModuleByUdf
import torch.utils.data as Data
from torch import tensor


"""
Time       : 2021/7/19 2:52 下午
Author     : zjchen
Description: 1. 最基本的功能是给trainOperator提供dataloader, 具体来说是将udf中的feature和label转换成dataloader，
                这一步还体现了平台无关的特性，因为用户的udf只需要将处理好的feature和label传入即可
             2. 其他用于用户自定义的数据预处理（不容易做抽象的预处理算子可以直接舍弃了）
             3. 
InputData  : pd.read_csv读取的dataframe对象
outputData : torch.utils.data.dataloader对象
udf的函数签名: def dataPrecessFunc(pd.dataFrame) -> tensor, tensor 
"""


logger = Logger('OperatorLogger').logger


class DataLoadOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataLoadOperator", ID, inputKeys, outputKeys, Params)
        # self.module = getModuleByUdf(self.params["udfPath"])

    def execute(self):
        try:
            # 同train中的参数，udf中的名称要固定
            # feature, label = self.module.dataPreProcess(self.getInputData("data"))
            feature, label = self.getInputData("data")[0], self.getInputData("data")[1]
            # Dataset接收tensor类型，TODO：tensor中有一些可选参数
            train_set = Data.TensorDataset(tensor(feature), tensor(label))
            train_iter = Data.DataLoader(train_set, self.params["batch_size"], shuffle=bool(self.params["shuffle"]))
            self.setOutputData("result", train_iter)

        except Exception as e:
            logger.error(traceback.format_exc())
