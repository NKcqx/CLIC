from abc import ABCMeta, abstractmethod

"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午4:44
@Author     : zjchen
"""


class OperatorFactory(metaclass=ABCMeta):
    """
    Description:
        PythonPlatform的OperatorFactory抽象基类，要求子类必须实现createOperator这个方法
    Attributes:
        /
    """
    @abstractmethod
    def createOperator(self, name, id, inputKeys, outputKeys, params):
        pass
