package fdu.daslab.backend.executor.model;

import fdu.daslab.backend.executor.utils.YamlUtil;

import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/20 10:02 上午
 */
public class Workflow {
    List<ArgoNode> tasks;

    // image列表
    List<ImageTemplate> imageTemplateList;

    // 适配器
    OperatorAdapter adapter;

    // 原始的operator
    List<?> stages;

    public Workflow(OperatorAdapter adapter, List<?> stages) { // List of stage
        this.adapter = adapter;
        this.stages = stages;
        // 适配
        // this.tasks = this.adapter.groupContinuousOperator(this.stages);
        this.tasks = this.adapter.adaptOperator(stages);
        this.imageTemplateList = this.adapter.generateTemplateByConfig();
    }

    public void execute() {
        // 1.组装DAG成一个yaml文件，并保存下本地
        YamlUtil yamlUtil = new YamlUtil();
        String path = yamlUtil.createArgoYaml(tasks, imageTemplateList);
        // 2.调用argo server api提交post请求
        // HttpUtil.submitPipelineByYaml(path);
    }
}
