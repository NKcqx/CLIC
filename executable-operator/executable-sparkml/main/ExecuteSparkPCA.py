import Executor
from basic.SparkOperatorFactory import SparkOperatorFactory
from utils.SparkInitUtil import SparkInitUtil
from pyspark.conf import SparkConf
import random
import time
import datetime

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:48
@Author     : jimmy
@Description: 使用CLIC中已扩展的算子完成PCA
"""


def RandID():
    return random.seed(time.process_time())


if __name__ == "__main__":
    program_start_time = datetime.datetime.now()
    cols = "hotel,is_canceled,lead_time,arrival_date_year,arrival_date_month,arrival_date_week_number," \
           "arrival_date_day_of_month,stays_in_weekend_nights,stays_in_week_nights,adults,children," \
           "babies,meal,country,market_segment,distribution_channel,is_repeated_guest," \
           "previous_cancellations,previous_bookings_not_canceled,reserved_room_type,assigned_room_type," \
           "booking_changes,deposit_type,agent,company,days_in_waiting_list,customer_type," \
           "adr,required_car_parking_spaces,total_of_special_requests,reservation_status"

    # 初始化OperatorFactory
    factory = SparkOperatorFactory()

    # 手动初始化所有Operator
    # 构造 SparkSession
    conf = SparkConf().setAppName("CLIC_PCA") \
        .setMaster("local") \
        .set('spark.executor.memory', '8g') \
        .set('spark.driver.memory', '8g') \
        .set("spark.memory.fraction", 0.8) \
        .set("spark.sql.shuffle.partitions", "800") \
        .set("spark.memory.offHeap.enabled", "true") \
        .set("spark.memory.offHeap.size", "2g") \
        .set("spark.sql.debug.maxToStringFields", 1000)
    spark = SparkInitUtil(conf=conf)

    # 读取csv文件
    source = factory.createOperator("SparkReadCSV", RandID(), [], ["result"],
                                    {"inputPath": r"E:\Project\CLIC_ML\data\Data_PCA\hotel_bookings.csv",
                                     "header": True,
                                     "infer_schema": True,
                                     "nan_value": "NULL"})
    # 去除无用列
    df_drop = factory.createOperator("DataframeDrop", RandID(), ["data"], ["result"],
                                     {"drop_cols": "reservation_status_date"})

    # 提取部分dataframe
    df_part = factory.createOperator("DataframeLimit", RandID(), ["data"], ["result"], {"number": "50"})

    # 标准化
    standard_scaler = factory.createOperator("SparkStandardScaler", RandID(), ["data"], ["result"],
                                             {"cols": cols, "handle_invalid": "keep"})

    # 空值填充
    fill_na = factory.createOperator("DataframeFillNa", RandID(), ["data"], ["result"], {"value": "0"})

    # onehot编码
    onehot_encoder = factory.createOperator("SparkOneHotEncode", RandID(), ["data"], ["result"], {"cols": cols})

    # PCA特征提取
    pca = factory.createOperator("SparkPCA", RandID(), ["data"], ["result"],
                                 {"k": "10", "output_col": "PCA_res", "cols": cols, "handle_invalid": "skip"})

    # 手动构建DAG图
    source.connectTo("result", df_drop, "data")
    df_drop.connectFrom("data", source, "result")

    df_drop.connectTo("result", df_part, "data")
    df_part.connectFrom("data", df_drop, "result")

    df_part.connectTo("result", standard_scaler, "data")
    standard_scaler.connectFrom("data", df_part, "result")

    standard_scaler.connectTo("result", fill_na, "data")
    fill_na.connectFrom("data", standard_scaler, "result")

    fill_na.connectTo("result", onehot_encoder, "data")
    onehot_encoder.connectFrom("data", fill_na, "result")

    onehot_encoder.connectTo("result", pca, "data")
    pca.connectFrom("data", pca, "result")

    # 拓扑排序
    Executor.execute([source])

    # 任务结束
    program_end_time = datetime.datetime.now()
    res = pca.getOutputData("result")
    res.show()
    print(res.count())

    print("Finish!")
    print("Total time in program: {} s.".format(str((program_end_time - program_start_time).total_seconds())))


