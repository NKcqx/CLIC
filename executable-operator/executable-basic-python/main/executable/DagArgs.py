
def parserArgD(d):
    """将platformArg解析成字典"""
    result = {}
    for pair in d:
        arg, value = pair.split("=")
        result[arg.lstrip("-")] = value
    return result


class DagArgs(object):
    """
    Description:
        Dag的参数，要求在创建Dag的时候传入(Command Line)。 args是parse_known_args()的返回
    Attributes:
        1. stageId      : 唯一的stageId
        2. dagPath      : 需要创建dag的Yaml文件路径
        3. notifyHost   : master的地址，提供给thrift实现远程调用
        4. notifyPort   : master启动的端口，提供给thrift实现远程调用
        5. platformArgs : 不同平台可能需要的参数，提供给DagHook执行额外的操作
        6. jobName      : 任务名称
    """
    def __init__(self, args):
        self.stageId = int(args[0].stageId)
        self.jobName = str(args[0].jobName)
        self.udfPath = str(args[0].udfPath)
        self.dagPath = str(args[0].dagPath)
        self.notifyHost = str(args[0].notifyHost)  # master的thrift地址
        self.notifyPort = int(args[0].notifyPort)  # master的thrift端口
        self.platformArgs = parserArgD(args[1]) if args[1] is not None else None  # 如果有未知参数，那么全都解析给platformArgs


