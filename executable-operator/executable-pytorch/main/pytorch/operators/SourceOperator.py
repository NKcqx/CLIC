import pandas as pd
import traceback
import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
from pytorch.basic.utils import getModuleByUdf
from executable.basic.utils.Logger import Logger
"""
@ProjectName: CLIC
@Time       : 2020/11/23 下午6:35
@Author     : zjchen
@Description: 读csv文件，文件路径参数为inputPath, 输出结果result
"""

logger = Logger('SourceOperatorLogger').logger


class SourceOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SourceOperator", ID, inputKeys, outputKeys, Params)
        self.module = getModuleByUdf(self.params["udfPath"]) if "udfPath" in self.params.keys() else None

    def execute(self):
        # 默认为方式为read_path
        if "type" not in self.params.keys():
            self.params["type"] = "read_path"

        if self.params["type"] == "read_path":
            self.setOutputData("result", self.params["inputPath"])
        elif self.params["type"] == "udf":
            self.setOutputData("result", self.module.sourceUdf(self.params["inputPath"]))


