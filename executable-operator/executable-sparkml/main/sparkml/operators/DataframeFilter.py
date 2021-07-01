import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/12/9 15:17
@Author     : Jimmy
@Description: 对Dataframe按条件过滤
"""

logger = Logger('OperatorLogger').logger


class DataframeFilter(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeFilter", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            condition = self.params["condition"]

            self.setOutputData("result", df.filter(condition))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
