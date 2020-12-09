import Executor
from basic.SparkOperatorFactory import SparkOperatorFactory
from utils.SparkInitUtil import SparkInitUtil
from pyspark.conf import SparkConf
from pyspark.sql import functions
import random
import time

"""
@ProjectName: CLIC
@Time       : 2020/12/8 16:35
@Author     : jimmy
@Description: 线性回归demo
"""


def RandID():
    return random.seed(time.process_time())


if __name__ == "__main__":
    start = time.time()
    cols = ['MSSubClass', 'MSZoning', 'LotFrontage', 'LotArea', 'Street', 'Alley', 'LotShape', 'LandContour',
            'Utilities', 'LotConfig', 'LandSlope', 'Neighborhood', 'Condition1', 'Condition2', 'BldgType', 'HouseStyle',
            'OverallQual', 'OverallCond', 'YearBuilt', 'YearRemodAdd', 'RoofStyle', 'RoofMatl', 'Exterior1st',
            'Exterior2nd', 'MasVnrType', 'MasVnrArea', 'ExterQual', 'ExterCond', 'Foundation', 'BsmtQual', 'BsmtCond',
            'BsmtExposure', 'BsmtFinType1', 'BsmtFinSF1', 'BsmtFinType2', 'BsmtFinSF2', 'BsmtUnfSF', 'TotalBsmtSF',
            'Heating', 'HeatingQC', 'CentralAir', 'Electrical', '1stFlrSF', '2ndFlrSF', 'LowQualFinSF', 'GrLivArea',
            'BsmtFullBath', 'BsmtHalfBath', 'FullBath', 'HalfBath', 'BedroomAbvGr', 'KitchenAbvGr', 'KitchenQual',
            'TotRmsAbvGrd', 'Functional', 'Fireplaces', 'FireplaceQu', 'GarageType', 'GarageYrBlt', 'GarageFinish',
            'GarageCars', 'GarageArea', 'GarageQual', 'GarageCond', 'PavedDrive', 'WoodDeckSF', 'OpenPorchSF',
            'EnclosedPorch', '3SsnPorch', 'ScreenPorch', 'PoolArea', 'PoolQC', 'Fence', 'MiscFeature', 'MiscVal',
            'MoSold', 'YrSold', 'SaleType', 'SaleCondition']

    # 初始化OperatorFactory
    factory = SparkOperatorFactory()

    # 手动初始化所有Operator
    # 构造 SparkSession
    conf = SparkConf().setAppName("CLIC_LR") \
        .setMaster("local[*]") \
        .set('spark.sql.codegen.wholeStage', False)

    spark = SparkInitUtil(conf=conf)

    # 读取csv文件
    source_train = factory.createOperator("SparkReadCSV", RandID(), [], ["result"],
                                          {"input_path": r"E:\Project\CLIC_ML\data\Data_HousePrice\train.csv",
                                           "header": True,
                                           "infer_schema": True,
                                           "nan_value": "NA"})

    source_test = factory.createOperator("SparkReadCSV", RandID(), [], ["result"],
                                         {"input_path": r"E:\Project\CLIC_ML\data\Data_HousePrice\test.csv",
                                          "header": True,
                                          "infer_schema": True,
                                          "nan_value": "NA"})

    # test新建一列
    with_column_test = factory.createOperator("DataframeWithColumn", RandID(), ["data"], ["result"],
                                              {"col_name": "SalePrice",
                                               "col": functions.lit(0)})

    # 合并两个Dataframe
    union = factory.createOperator("DataframeUnion", RandID(), ["data_1", "data_2"], ["result"], {})

    # 标准化
    standard = factory.createOperator("SparkStandardScaler", RandID(), ["data"], ["result"], {"cols": cols})

    # 空值填充
    fill_na = factory.createOperator("DataframeFillNa", RandID(), ["data"], ["result"], {"value": 0})

    # onehot编码
    onehot = factory.createOperator("SparkOneHotEncode", RandID(), ["data"], ["result"], {"cols": cols})

    # 所有特征合成一列
    assembler = factory.createOperator("SparkVectorAssembler", RandID(), ["data"], ["result"],
                                       {"input_cols": cols, "output_col": "features"})

    # 分离train和test数据集
    train_data = factory.createOperator("DataframeFilter", RandID(), ["data"], ["result"],
                                        {"condition": "Id >= 1 and Id <= 1460"})

    test_data = factory.createOperator("DataframeFilter", RandID(), ["data"], ["result"],
                                       {"condition": "Id > 1460 and Id <= 2919"})

    # 去除test多余行
    test_drop = factory.createOperator("DataframeDrop", RandID(), ["data"], ["result"], {"drop_cols": ["SalePrice"]})

    # 训练
    lr_train = factory.createOperator("SparkLinearRegression", RandID(), ["train_data"], ["result"],
                                      {"feature_col": "features",
                                       "label": "SalePrice",
                                       "predict_col": "predictPrice",
                                       "max_iter": 20,
                                       "reg_param": 0.3,
                                       "standardization": True,
                                       "elastic_net_param": 0.8})

    # 预测
    lr_predict = factory.createOperator("SparkTransform", RandID(), ["model", "data"], ["result"], {})

    # 手动构建DAG图
    source_train.connectTo("result", union, "data_1")
    union.connectFrom("data_1", source_train, "result")

    source_test.connectTo("result", with_column_test, "data")
    with_column_test.connectFrom("data", source_test, "result")

    with_column_test.connectTo("result", union, "data_2")
    union.connectFrom("data_2", with_column_test, "result")

    union.connectTo("result", standard, "data")
    standard.connectFrom("data", union, "result")

    standard.connectTo("result", fill_na, "data")
    fill_na.connectFrom("data", standard, "result")

    fill_na.connectTo("result", onehot, "data")
    onehot.connectFrom("data", fill_na, "result")

    onehot.connectTo("result", assembler, "data")
    assembler.connectFrom("data", onehot, "result")

    assembler.connectTo("result", train_data, "data")
    train_data.connectFrom("data", assembler, "result")

    train_data.connectTo("result", lr_train, "train_data")
    lr_train.connectFrom("train_data", train_data, "result")

    lr_train.connectTo("result", lr_predict, "model")
    lr_predict.connectFrom("model", lr_train, "result")

    assembler.connectTo("result", test_data, "data")
    test_data.connectFrom("data", assembler, "result")

    assembler.connectTo("result", test_data, "data")
    test_data.connectFrom("data", assembler, "result")

    test_data.connectTo("result", test_drop, "data")
    test_drop.connectFrom("data", test_data, "result")

    test_drop.connectTo("result", lr_predict, "data")
    lr_predict.connectFrom("data", test_drop, "result")

    # 拓扑排序
    Executor.execute([source_train, source_test])

    # 任务结束
    end = time.time()
    res = lr_predict.getOutputData("result")
    res.show()
    print(res.count())

    print("Finish!")
    print("Start: " + str(time.localtime(start)))
    print("End: " + str(time.localtime(end)))
