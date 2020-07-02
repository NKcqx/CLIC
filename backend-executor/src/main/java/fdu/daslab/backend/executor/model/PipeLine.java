package fdu.daslab.backend.executor.model;

import fdu.daslab.backend.executor.utils.HttpUtil;

import java.util.List;

/**
 * 定义一个可以在argo上实际运行所需要的参数的pipeline
 */
public class PipeLine {

    // 任务列表
    List<ArgoNode> tasks;

    // 适配器
    OperatorAdapter adapter;

    // 原始的operator
    List<Object> operators;

    public PipeLine(OperatorAdapter adapter, List<Object> operators) {
        this.adapter = adapter;
        this.operators = operators;
        // 适配
        this.tasks = this.adapter.groupContinuousOperator(this.operators);
    }

    /**
     * 执行pipeline
     */
    void execute() {
        // 提交pipeline
        HttpUtil.submitPipeline(this.tasks);
    }
}
