import traceback
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
"""
@ProjectName: CLIC
@Time       : 2020/12/13 上午11:42
@Author     : zjchen
@Description: 根据列名获得DataFrame中的某一列，返回结果是Series格式
"""

logger = Logger('OperatorLogger').logger


class PdGetSeries(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdGetSeries", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", self.getInputData("data")[self.params["value"]])
        except Exception as e:
            logger.error(traceback.format_exc())

