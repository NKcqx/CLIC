import os
import sys
import importlib
import traceback
from executable.basic.utils.Logger import Logger

"""
Time       : 2021/7/21 10:49 上午
Author     : zjchen
Description:
"""


logger = Logger('getUdfLogger').logger


def splitUdfPath(udfPath):
    filePath, fileName = os.path.split(udfPath)
    name, extension = os.path.splitext(fileName)
    return filePath, name, extension


def getModuleByUdf(udfPath):
    try:
        if udfPath is None:
            raise ValueError("udfPath不能为空！")
        # 获取模块路径
        filePath, name, extension = splitUdfPath(udfPath)
        sys.path.append(filePath)
        return importlib.import_module(name)
    except Exception as e:
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    moduleA = getModuleByUdf("/Users/zjchen/Codes/JavaProjects/CLIC/executable-operator/executable-pytorch/test/pytorch/udf/TestSenti.py")
    print(dir(moduleA))
    print(type(moduleA.loss))
