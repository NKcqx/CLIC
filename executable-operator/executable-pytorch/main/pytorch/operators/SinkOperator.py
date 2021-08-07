import traceback

import pandas as pd

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/12/21 下午6:49
@Author     : zjchen
@Description: 将tensor或dataFrame写CSV文件
"""

logger = Logger('OperatorLogger').logger


class SinkOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SinkOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            data = self.getInputData("data")
            if self.params["type"] == "dataFrame":
                pass
            elif self.params["type"] == "tensor":
                data = pd.DataFrame(data.numpy())
            data.to_csv(self.params["outputPath"])

            self.setOutputData("result", "Successful!")

        except Exception as e:
            logger.error(traceback.format_exc())
