package fdu.daslab.backend.executor.model;

import fdu.daslab.backend.executor.utils.HttpUtil;
import fdu.daslab.backend.executor.utils.YamlUtil;

import java.util.List;

/**
 * 定义一个可以在argo上实际运行所需要的参数的pipeline
 */
public class Pipeline {

    // 任务列表
    List<ArgoNode> tasks;

    // 适配器
    OperatorAdapter adapter;

    // 原始的operator
    List<?> operators;

    public Pipeline(OperatorAdapter adapter, List<?> operators) {
        this.adapter = adapter;
        this.operators = operators;
        // 适配
        this.tasks = this.adapter.groupContinuousOperator(this.operators);
    }

    /**
     * 执行pipeline
     */
    public void execute() {
        // 1.组装DAG成一个yaml文件，并保存下本地
        YamlUtil yamlUtil = new YamlUtil();
        String path = yamlUtil.createArgoYaml(tasks);
        // 2.调用argo server api提交post请求
        HttpUtil.submitPipelineByYaml(path);
    }
}
