from operators.DataframeUnion import DataframeUnion
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as ft
"""
@ProjectName: CLIC
@Time       : 2020/11/26 16:13
@Author     : jimmy
@Description: 
"""


def readCsv(spark, path, header=True, inferSchema=True, nanValue='NA'):
    return spark.read.csv(path=path, header=header, inferSchema=inferSchema, nanValue=nanValue)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('HousePricePredict') \
        .master('local') \
        .getOrCreate()

    sc = SparkContext.getOrCreate()

    training_data = readCsv(spark=spark, path=r"E:\Project\CLIC_ML\data\Data_HousePrice\train.csv",
                            header=True, inferSchema=True, nanValue='NA')
    test_data = readCsv(spark=spark, path=r"E:\Project\CLIC_ML\data\Data_HousePrice\test.csv",
                        header=True, inferSchema=True, nanValue='NA')

    input_data = dict()
    input_data['input_Data_1'] = training_data.limit(2)
    input_data['input_Data_2'] = test_data.limit(2)

    dfu = DataframeUnion(ID=1, inputKeys=['input_Data_1', 'input_Data_2'], outputKeys=['result'], Params=[])
    dfu.inputData['input_Data_1'] = training_data.limit(2)
    dfu.inputData['input_Data_2'] = test_data.withColumn("SalePrice", ft.lit(0)).limit(2)

    dfu.execute()
    dfu.outputData['result'].show()
