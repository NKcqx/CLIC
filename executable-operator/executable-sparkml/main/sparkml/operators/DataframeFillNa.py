import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:49
@Author     : Jimmy
@Description: 对dataframe的空值填充指定值
"""

logger = Logger('OperatorLogger').logger


class DataframeFillNa(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeFillNa", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            value = eval(self.params["value"])

            self.setOutputData("result", df.fillna(value))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())

