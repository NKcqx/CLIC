package fdu.daslab.backend.executor.model;

import fdu.daslab.backend.executor.utils.HttpUtil;
import fdu.daslab.backend.executor.utils.YamlUtil;

import java.util.List;

/**
 * 定义一个可以在argo上实际运行所需要的参数的pipeline
 *
 * @author 杜清华，唐志伟
 * @since  2020/7/6 11:39
 * @version 1.0
 */
public class Pipeline {

    // 任务列表
    List<ArgoNode> tasks;

    // image列表
    List<ImageTemplate> imageTemplateList;

    // 适配器
    OperatorAdapter adapter;

    // 原始的operator
    List<?> operators;

    public Pipeline(OperatorAdapter adapter, List<?> operators) {
        this.adapter = adapter;
        this.operators = operators;
        // 适配
        this.tasks = this.adapter.groupContinuousOperator(this.operators);
        this.imageTemplateList = this.adapter.generateTemplateByConfig();
    }

    /**
     * 执行pipeline
     */
    public void execute() {
        // 1.组装DAG成一个yaml文件，并保存下本地
        YamlUtil yamlUtil = new YamlUtil();
        String path = yamlUtil.createArgoYaml(tasks, imageTemplateList);
        // 2.调用argo server api提交post请求
        HttpUtil.submitPipelineByYaml(path);
    }
}
