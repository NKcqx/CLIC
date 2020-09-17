package fdu.daslab.executable.basic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 所有Opt的基类，提供基础的属性和链接操作
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/17 3:52 下午
 */
public abstract class OperatorBase<InputType, OutputType> implements ExecutionOperator<OutputType> {
    protected String name;
    protected String id;
    protected Map<String, InputType> inputData; // 输入数据
    protected Map<String, OutputType> outputData; // 输出数据
    protected Map<String, String> params; // （输入）参数值
    protected List<Connection> outputConnections; // 所有的下一跳
    protected List<Connection> inputConnections; // 所有的上一跳
    protected int inDegree = 0; // 入度，用于拓扑排序

    public List<Connection> getOutputConnections() {
        return outputConnections;
    }

    public List<Connection> getInputConnections() {
        return inputConnections;
    }

    public OperatorBase(String name, String id, List<String> inputKeys,
                        List<String> outputKeys, Map<String, String> params) {
        this.name = name;
        this.id = id;

        this.inputData = new HashMap<>();
        for (String key : inputKeys) {
            this.inputData.put(key, null);
        }
        this.outputData = new HashMap<>();
        for (String key : outputKeys) {
            this.outputData.put(key, null);
        }
        this.params = params;
        outputConnections = new ArrayList<>();
        inputConnections = new ArrayList<>();
    }

    public void connectTo(String sourceKey, OperatorBase targetOpt, String targetKey) {
        outputConnections.add(new Connection(this, sourceKey, targetOpt, targetKey));
    }

    public void connectFrom(String targetKey, OperatorBase sourceOpt, String sourceKey) {
        inputConnections.add(new Connection(sourceOpt, sourceKey, this, targetKey));
    }

    public int getInDegree() {
        return inDegree;
    }

    public Map<String, String> getParams() {
        return params;
    }


    public String getName() {
        return name;
    }



    public void setInputData(String key, InputType data) {
        this.inputData.put(key, data);
    }

    public InputType getInputData(String key) {
        return this.inputData.get(key);
    }

    public OutputType getOutputData(String key) {
        return this.outputData.get(key);
    }

    public void setOutputData(String key, OutputType data) {
        this.outputData.put(key, data);
    }

    /**
     * 更新节点的入度
     *
     * @param delta 入度的变化值，为 0 时表示初始化入度，为其他值时则直接更新
     * @return 当前节点的入度值
     */
    public int updateInDegree(int delta) {
        if (delta == 0) { // 初始化
            this.inDegree = this.inputConnections.size();
        } else {
            this.inDegree = this.inDegree + delta;
        }
        return this.inDegree;
    }

}
