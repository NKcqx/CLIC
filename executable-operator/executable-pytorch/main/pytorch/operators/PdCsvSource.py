import pandas as pd
import traceback
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
"""
@ProjectName: CLIC
@Time       : 2020/11/23 下午6:35
@Author     : zjchen
@Description: 读csv文件，文件路径参数为inputPath, 输出结果result
"""

logger = Logger('OperatorLogger').logger


class PdCsvSource(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdCsvSource", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", pd.read_csv(self.params["inputPath"]))
        except Exception as e:
            logger.error(traceback.format_exc())

