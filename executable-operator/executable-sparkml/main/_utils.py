from queue import SimpleQueue
import yaml
import basic.SparkOperatorFactory as SparkOperatorFactory

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


def parseYAML(file_path):
    with open(file_path) as file:
        job = yaml.load(file, Loader=yaml.FullLoader)
    factory = SparkOperatorFactory.SparkOperatorFactory()
    opt_dict = {yaml_opt["id"]: factory.createOperator(
        yaml_opt["name"], 
        yaml_opt["id"], 
        yaml_opt["inputKeys"],
        yaml_opt["outputKeys"], 
        yaml_opt["params"])
        for yaml_opt in job["operators"]}

    head_opt = None
    for edge in job["dag"]:
        source_opt = opt_dict[edge["id"]]
        if "dependencies" not in edge:
            head_opt = source_opt
        else:
            for dependency in edge["dependencies"]:
                target_opt = opt_dict[dependency["id"]]
                source_opt.connectTo(
                    dependency["sourceKey"], 
                    target_opt,
                    dependency["targetKey"])
                target_opt.connectFrom(
                    dependency["targetKey"], 
                    source_opt, 
                    dependency["sourceKey"])     
    return head_opt