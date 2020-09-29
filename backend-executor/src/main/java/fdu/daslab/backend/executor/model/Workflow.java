package fdu.daslab.backend.executor.model;

//import fdu.daslab.backend.executor.utils.HttpUtil;
import fdu.daslab.backend.executor.utils.KubernetesUtil;
import fdu.daslab.backend.executor.utils.YamlUtil;

import java.util.List;
import java.util.Map;

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


    public Map<Integer, KubernetesStage> execute() {
        // 1.组装DAG成一个yaml文件，并保存下本地
        YamlUtil yamlUtil = new YamlUtil();
        String yamlPath = yamlUtil.createArgoYaml(tasks, imageTemplateList);
        // 2.目前的yaml只用于描述dag，不作为实际运行需要，实际运行依赖于k8s，并返回每一个pod的ip:port和对应的依赖关系
        return KubernetesUtil.createStagePodAndGetStageInfo(yamlPath);

        // 2.调用argo server api提交post请求
//        HttpUtil.submitPipelineByYaml(path);
    }
}
