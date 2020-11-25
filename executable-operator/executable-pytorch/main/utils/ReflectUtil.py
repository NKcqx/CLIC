import importlib
from utils.MethodGetter import MethodGetter
from model.FunctionModel import FunctionModel

"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午10:08
@Author     : zjchen
@Description: 
"""


class ReflectUtil:
    @staticmethod
    def createInstanceAndMethodByPath(path):
        try:
            paths = path.split("/")
            if paths[len(paths) - 1].endswith(".py"):
                className = paths[len(paths) - 1][0:-3]
            else:
                raise ValueError("ReflectUtil下的path解析错误")
            # 动态加载某个模块，并加载其中的类，这里类名和模块名应该相同，否则会出Bug
            module = importlib.import_module("udf." + className)
            module_class = getattr(module, className)
            if module_class is not None:
                try:
                    functions = {}
                    for method in MethodGetter(module_class).getMethodList():
                        functions[method] = getattr(module_class, method)
                    return FunctionModel(module_class(), functions)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
        return None


if __name__ == '__main__':
    ReflectUtil.createInstanceAndMethodByPath("/Users/zjchen/IdeaProjects/CLIC/executable-operator/executable-pytorch"
                                              "/udf/TestUdf.py")