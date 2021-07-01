import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:46
@Author     : Jimmy
@Description: 删除dataframe的指定列
"""

logger = Logger('OperatorLogger').logger


class DataframeDrop(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeDrop", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            drop_cols = self.params["dropCols"]    # 要删除多个列用逗号隔开
            drop_cols = [x.strip() for x in drop_cols.split(',')]

            for col in drop_cols:
                df = df.drop(col)

            self.setOutputData("result", df)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
