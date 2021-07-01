import traceback

from pyspark.ml.regression import LinearRegression
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:24
@Author     : Jimmy
@Description: Spark ML 线性回归
"""

logger = Logger('OperatorLogger').logger


class SparkLinearRegression(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkLinearRegression", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            train_data = self.getInputData("data")
            feature_col = self.params["featureCol"]
            label = self.params["label"]

            # 定义线性回归模型
            lr = LinearRegression() \
                .setFeaturesCol(feature_col) \
                .setPredictionCol(self.params["predictCol"]) \
                .setLabelCol(label) \
                .setFitIntercept(True) \
                .setMaxIter(int(self.params["maxIter"])) \
                .setRegParam(float(self.params["regParam"])) \
                .setElasticNetParam(float(self.params["elasticNetParam"])) \
                .setStandardization(True if str(self.params["standardization"]).lower() == 'true' else False)

            lr_model = lr.fit(train_data)

            self.setOutputData("result", lr_model)

            lr_model.transform(train_data)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
