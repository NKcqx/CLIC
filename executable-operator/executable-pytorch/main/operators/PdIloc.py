import pandas as pd
import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午11:48
@Author     : zjchen
@Description: 
"""


class PdIloc(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdIloc", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            dataFrame = pd.DataFrame(self.getInputData("input_DataFrame"))
            dataFrame = dataFrame.iloc[self.params["row_from"]: self.params["row_to"],
                                       self.params["col_from"]: self.params["col_to"]]
            self.setOutputData("result", dataFrame)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
