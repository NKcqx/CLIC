import yaml


def parse_args(file_path, operator_factory):
    """
    Description:
        用来解析dagPath，并根据dag图把operator连接，返回结果是dag的head节点
    Args:
        1. file_path(str)  : dagPath的路径
        2. operator_factory: 当前dagPlatform的OperatorFactory
    Returns:
        list类型的所有头节点(operator)
    """
    with open(file_path) as file:
        job = yaml.load(file, Loader=yaml.FullLoader)
    opt_dict = parseOperator(job["operators"], operator_factory)
    return parseEdge(job["dag"], opt_dict)


def parseOperator(opt_yaml_list, factory):
    # factory = PytorchOperatorFactory.PytorchOperatorFactory()
    opt_dict = {yaml_opt["id"]: factory.createOperator(
        yaml_opt["name"],
        yaml_opt["id"],
        yaml_opt["inputKeys"],
        yaml_opt["outputKeys"],
        yaml_opt["params"])
        for yaml_opt in opt_yaml_list}
    return opt_dict


def parseEdge(edge_list, opt_pool):
    head_opt = []
    for edge in edge_list:
        cur_opt = opt_pool[edge["id"]]
        if "dependencies" not in edge:
            head_opt.append(cur_opt)
        else:
            for dependency in edge["dependencies"]:
                source_opt = opt_pool[dependency["id"]]
                source_opt.connectTo(
                    dependency["sourceKey"],
                    cur_opt,
                    dependency["targetKey"])
                cur_opt.connectFrom(
                    dependency["targetKey"],
                    source_opt,
                    dependency["sourceKey"])
    return head_opt
