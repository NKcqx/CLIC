import traceback

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType

from model.OperatorBase import OperatorBase
from pyspark.sql.functions import split, regexp_replace
"""
@ProjectName: CLIC
@Time       : 2020/11/25 18:44
@Author     : jimmy
@Description: 对dataframe的指定列中的数值型列做标准化
"""


class SparkStandardScaler(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkStandardScaler", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            cols = self.params["cols"]
            numeric_types = ['int', 'long', 'double', 'float']  # TODO 待补充
            numeric_features = [item[0] for item in df.dtypes if item[1] in numeric_types and item[0] in cols]

            # 对数字类型的特征标准化
            for feature in numeric_features:
                assembler = VectorAssembler(inputCols=[feature], outputCol=feature + '-ass', handleInvalid='keep')
                scaler = StandardScaler(inputCol=feature + '-ass', outputCol=feature + '-scaler', withMean=True)
                pipeline = Pipeline(stages=[assembler, scaler])
                df = pipeline.fit(df) \
                    .transform(df) \
                    .drop(feature) \
                    .drop(feature + '-ass')

                # 由于标准化后列是vector型，需转成double型
                df = df.withColumn(feature + '-str', df[feature + '-scaler'].cast("String")) \
                    .drop(feature + '-scaler')
                df = df \
                    .withColumn(feature + '-res',split(regexp_replace(feature + '-str', "^\[|\]", ""), ",")[0]
                                .cast(DoubleType())) \
                    .drop(feature + '-str') \
                    .withColumnRenamed(feature + '-res', feature)

            self.setOutputData("result", df)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
