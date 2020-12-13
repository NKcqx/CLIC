
"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午10:07
@Author     : zjchen
@Description: 传入一个Class，用列表的形式传出它所有的方法
"""
import inspect



class MethodGetter:
    def __init__(self, classToGetMethod):
        self.classToGetMethod = classToGetMethod
        self.methodList = []

    def getMethodList(self):
        for method, value in inspect.getmembers(self.classToGetMethod):
            if not method.startswith("_"):
                self.methodList.append(method)
        return self.methodList


if __name__ == "__main__":
    # pass
    print(MethodGetter(TestUdf).getMethodList())
