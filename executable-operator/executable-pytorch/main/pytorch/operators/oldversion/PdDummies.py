import traceback
import pandas as pd
from executable.basic.model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午4:27
@Author     : zjchen
@Description: 对目标dataFrame做one hot encode
"""


class PdDummies(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdDummies", ID, inputKeys, outputKeys, Params)

    def execute(self):

        self.setOutputData("result", pd.get_dummies(self.getInputData("data"),
                                                    dummy_na=(self.params["dummy_na"].lower() == 'true')
                                                    ))

