import traceback

from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:24
@Author     : jimmy
@Description: Spark ML库线性回归
"""


class SparkLinearRegression(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkLinearRegression", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            train_data = self.getInputData("train_data")
            feature_col = self.params["feature_col"]
            label = self.params["label"]

            # 定义线性回归模型
            lr = LinearRegression() \
                .setFeaturesCol(feature_col) \
                .setPredictionCol(self.params["predict_col"]) \
                .setLabelCol(label) \
                .setFitIntercept(True) \
                .setMaxIter(self.params["max_iter"]) \
                .setRegParam(self.params["reg_param"]) \
                .setElasticNetParam(self.params["elastic_net_param"]) \
                .setStandardization(self.params["standardization"])

            # 建立管道
            # pipeline_lr = Pipeline(stages=[lr])
            lr_model = lr.fit(train_data)

            self.setOutputData("result", lr_model)

            lr_model.transform(train_data)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
