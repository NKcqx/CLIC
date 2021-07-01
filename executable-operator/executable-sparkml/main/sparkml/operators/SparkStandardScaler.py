import traceback

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType, FloatType, ShortType, ByteType, DecimalType, IntegerType, LongType

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.sql.functions import split, regexp_replace
"""
@ProjectName: CLIC
@Time       : 2020/11/25 18:44
@Author     : Jimmy
@Description: Spark ML 对dataframe中的数值型列标准化
"""

logger = Logger('OperatorLogger').logger


class SparkStandardScaler(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkStandardScaler", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            cols = self.params["cols"]
            cols = [x.strip() for x in cols.split(',')]
            handle_invalid = self.params["handleInvalid"] if "handleInvalid" in self.params else None
            handle_invalid = 'skip' if handle_invalid is not None and str(handle_invalid).lower() == 'skip' else 'keep'
            numeric_types = {
                'int': IntegerType(),
                'long': LongType(),
                'double': DoubleType(),
                'float': FloatType(),
                'short': ShortType(),
                'byte': ByteType(),
                'decimal': DecimalType()
            }

            # 对数字类型的特征标准化
            for item in df.dtypes:
                if item[1] in numeric_types.keys() and item[0] in cols:
                    feature = item[0]
                    assembler = VectorAssembler(inputCols=[feature],
                                                outputCol=feature + '-assembler',
                                                handleInvalid=handle_invalid)
                    scaler = StandardScaler(inputCol=feature + '-assembler',
                                            outputCol=feature + '-scaler',
                                            withMean=True)
                    pipeline = Pipeline(stages=[assembler, scaler])
                    df = pipeline.fit(df) \
                        .transform(df) \
                        .drop(feature) \
                        .drop(feature + '-assembler')

                    # 由于标准化后列是vector型，需转成double型
                    df = df.withColumn(feature + '-str', df[feature + '-scaler'].cast("String")) \
                        .drop(feature + '-scaler')
                    df = df \
                        .withColumn(feature + '-res', split(regexp_replace(feature + '-str', "^\[|\]", ""), ",")[0]
                                    .cast(numeric_types[item[1]])) \
                        .drop(feature + '-str') \
                        .withColumnRenamed(feature + '-res', feature)

            self.setOutputData("result", df)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
