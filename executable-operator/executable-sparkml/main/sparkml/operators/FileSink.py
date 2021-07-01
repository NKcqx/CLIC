import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/12/21 18:50
@Author     : Jimmy
@Description: 将Dataframe存到csv文件中，可选择直接输出（即输出Spark分布式文件）或者转换成 pandas 的 dataframe 后输出单一文件
"""

logger = Logger('OperatorLogger').logger


class FileSink(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("FileSink", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            data = self.getInputData("data")
            header = self.params["header"].lower()  # 是否保留列名
            if self.params["outputType"] == "spark":
                data.write.option("header", header).csv(self.params["outputPath"])
            elif self.params["outputType"] == "pandas":
                data.toPandas() \
                    .to_csv(self.params["outputPath"], header=(header == "true"), encoding='utf_8_sig', index=False)

            self.setOutputData("result", "Successful!")

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
