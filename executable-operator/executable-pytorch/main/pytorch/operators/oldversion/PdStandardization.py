from executable.basic.model.OperatorBase import OperatorBase


"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午2:02
@Author     : zjchen
@Description: 对输入的每一个dataframe进行标准化操作，返回一个修改完的字典
"""


class PdStandardization(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdStandardization", ID, inputKeys, outputKeys, Params)

    def execute(self):
        dataFrame = self.getInputData("data")
        toStandIndex = dataFrame.dtypes[dataFrame.dtypes != 'object'].index
        dataFrame[toStandIndex] = dataFrame[toStandIndex].apply(lambda x: (x - x.mean()) / (x.std()))
        self.setOutputData("result", dataFrame)



