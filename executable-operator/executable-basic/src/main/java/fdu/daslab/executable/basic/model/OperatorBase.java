package fdu.daslab.executable.basic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.*;

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
    protected String schema;
    protected Map<String, InputType> inputData; // 输入数据
    protected Map<String, OutputType> outputData; // 输出数据
    protected Map<String, String> params; // （输入）参数值
    protected List<Connection> outputConnections; // 所有的下一跳
    protected List<Connection> inputConnections; // 所有的上一跳
    protected int inDegree = 0; // 入度，用于拓扑排序
    protected OperatorState operatorState = OperatorState.Waiting; // 需要加锁

    public enum OperatorState {
        Waiting, Ready, Running, Finished
    }

    public OperatorState getOperatorState() {
        return operatorState;
    }

    public void setOperatorState(OperatorState operatorState) {
        this.operatorState = operatorState;
    }

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
        // 先检查 List中是否包含 相同的边（起点opt 和 终点 opt相同），相同的话直接往 Connection的 KeyList 中添加 key-pair
        for (Connection connection : outputConnections) {
            if (connection.getTargetOpt() == targetOpt) {
                connection.addKey(sourceKey, targetKey);
                return;
            }
        }
        outputConnections.add(new Connection(this, sourceKey, targetOpt, targetKey));
    }

    public void connectTo(Connection connection) throws Exception {
        if (connection.getSourceOpt() == this || connection.getTargetOpt() == this) {
            outputConnections.add(connection);
        } else {
            throw new Exception("Connection 的两端未包含当前Opt, Connection："
                    + connection.getSourceOpt().toString()
                    + " -> "
                    + connection.getTargetOpt().toString()
                    + "。当前Operator"
                    + this.toString());
        }
    }

    public void disconnectTo(Connection connection) {
        if (this.outputConnections.contains(connection)) {
            this.outputConnections.remove(connection);
        }
    }

    public void connectFrom(String targetKey, OperatorBase sourceOpt, String sourceKey) {
        for (Connection connection : inputConnections) {
            if (connection.getSourceOpt() == sourceOpt) {
                connection.addKey(sourceKey, targetKey);
                return;
            }
        }
        this.updateInDegree(1);
        inputConnections.add(new Connection(sourceOpt, sourceKey, this, targetKey));
    }

    public void connectFrom(Connection connection) throws Exception {
        if (connection.getSourceOpt() == this || connection.getTargetOpt() == this) {
            inputConnections.add(connection);
            this.updateInDegree(1);
        } else {
            throw new Exception("Connection 的两端未包含当前Opt, Connection："
                    + connection.getSourceOpt().toString()
                    + " -> "
                    + connection.getTargetOpt().toString()
                    + "。当前Operator"
                    + this.toString());
        }
    }

    public void disconnectFrom(Connection connection) {
        if (this.inputConnections.contains(connection)) {
            this.inputConnections.add(connection);
            this.updateInDegree(-1);
        }
    }

    public int getInDegree() {
        return inDegree;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(String key, String value) {
        this.params.put(key, value);
    }

    public String getName() {
        return name;
    }


    public void setInputData(String key, InputType data) { // todo 检查key是否存在！
        this.inputData.put(key, data);
        // 查找是否还有没有传入的输入数据
        boolean hasEmptyInputData = this.inputData.values().stream().anyMatch(Objects::isNull);
        if (!hasEmptyInputData) {
            this.setOperatorState(OperatorState.Ready);
        }
    }

    public InputType getInputData(String key) {
        return this.inputData.get(key);
    }

    public OutputType getOutputData(String key) {
        return this.outputData.get(key);
    }

    public void setOutputData(String key, OutputType data) {
        // todo 检查key是否存在！
        this.outputData.put(key, data);
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * 更新节点的入度
     *
     * @param delta 入度的变化值
     * @return 当前节点的入度值
     */
    public int updateInDegree(int delta) {
        this.inDegree = this.inDegree + delta;
        return this.inDegree;
    }

}
