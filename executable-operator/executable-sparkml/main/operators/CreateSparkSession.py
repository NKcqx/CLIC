import traceback

from model.OperatorBase import OperatorBase
from pyspark.sql import SparkSession

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:07
@Author     : jimmy
@Description: 
"""


class CreateSparkSession(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("CreateSparkSession", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            appName = self.params["app_name"]
            master = self.params["master"]

            self.setOutputData("result", SparkSession.builder
                               .appName(appName)
                               .master(master)
                               .getOrCreate())

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())