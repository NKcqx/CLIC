import yaml
import basic.PythonOperatorFactory as PythonOperatorFactory

def parse(file_path):
    with open(file_path) as file:
        job = yaml.load(file, Loader=yaml.FullLoader)
    opt_dict = parseOperator(job["operators"])
    return parseEdge(job["dag"], opt_dict)
    

def parseOperator(opt_yaml_list):
    factory = PythonOperatorFactory.PytorchOperatorFactory()
    opt_dict = {yaml_opt["id"]: factory.createOperator(
        yaml_opt["name"], 
        yaml_opt["id"], 
        yaml_opt["inputKeys"],
        yaml_opt["outputKeys"], 
        yaml_opt["params"])
        for yaml_opt in opt_yaml_list}
    return opt_dict

def parseEdge(edge_list, opt_pool):
    head_opt = None
    for edge in edge_list:
        source_opt = opt_pool[edge["id"]]
        if "dependencies" not in edge:
            head_opt = source_opt
        else:
            for dependency in edge["dependencies"]:
                target_opt = opt_pool[dependency["id"]]
                source_opt.connectTo(
                    dependency["sourceKey"], 
                    target_opt,
                    dependency["targetKey"])
                target_opt.connectFrom(
                    dependency["targetKey"], 
                    source_opt, 
                    dependency["sourceKey"])
                
    return head_opt
"""
with open("/Users/jason/Downloads/tmp/physical_pytorch.yml") as file:
    job = yaml.load(file, Loader=yaml.FullLoader)
opts = parseOperator(job["operator"])
head = parseEdge(job["dag"], opts)  
"""