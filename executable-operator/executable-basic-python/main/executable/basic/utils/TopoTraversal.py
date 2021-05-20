from queue import SimpleQueue

"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午1:05
@Author     : zjchen
"""


class TopoTraversal:
    """
    Description:
        执行Dag的拓扑排序类
    Attributes:
        queue(SimpleQueue): 用来实现拓扑排序的简单队列
    """
    def __init__(self, headList):
        self.queue = SimpleQueue()
        for head in headList:
            self.queue.put(head)

    def nextOpt(self):
        return self.queue.get()

    def updateInDegree(self, opt, delta):
        """更新节点的入度"""
        opt.updateInDegree(delta)  # 这里调用的是OperatorBase中的updateInDegree，所以参数只有一个
        if opt.getInDegree() <= 0:
            self.queue.put(opt)

    def hasNextOpt(self):
        return not self.queue.empty()
