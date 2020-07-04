package fdu.daslab.backend.executor.model;

import java.util.List;

/**
 * 定义argo的DAG上的一个node
 */
public class ArgoNode {

    // 参数定义
    public static class Parameter {

        String name;  // 参数名
        String value;   // 参数值

        public Parameter(String name, String value) {
            this.name = name;
            this.value = value;
        }
        public String getName() {
            return name;
        }
        public String getValue() {
            return value;
        }
    }

    // 该节点的名字
    String name;

    //节点编号，在每个job中，id是依次的
    int id;

    // 该节点使用的template（image）
    String template;

    // 该节点依赖的上游
    List<ArgoNode> dependencies;

    // 该节点的输入参数
    List<Parameter> parameters;

    public ArgoNode(int id, String name, String template, List<ArgoNode> dependencies, List<Parameter> parameters) {
        this.id=id;
        this.name = name;
        this.template = template;
        this.dependencies = dependencies;
        this.parameters = parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }
    public String getName() {
        return name;
    }

    public String getTemplate() {
        return template;
    }

    public List<ArgoNode> getDependencies() {
        return dependencies;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }
    public int getId() {
        return id;
    }
}