from queue import SimpleQueue
"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午1:05
@Author     : zjchen
@Description: 
"""


class TopoTraversal:
    def __init__(self, headList):
        self.queue = SimpleQueue()
        for head in headList:
            self.queue.put(head)

    def nextOpt(self):
        return self.queue.get()

    def updateInDegree(self, opt, delta):
        opt.updateInDegree(delta)
        if opt.getInDegree() <= 0:
            self.queue.put(opt)

    def hasNextOpt(self):
        return not self.queue.empty()
