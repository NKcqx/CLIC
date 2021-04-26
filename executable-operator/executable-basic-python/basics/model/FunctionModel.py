"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午10:02
@Author     : zjchen
@Description: 
"""


class FunctionModel:
    def __init__(self, obj, functionmap):
        self.obj = obj
        self.functionmap = functionmap
        self.method = None

    def invoke(self, functionName, *args):
        try:
            self.method = getattr(self.obj, self.functionmap[functionName])
            return self.method(args)
        except Exception as e:
            print(e)
        return None
