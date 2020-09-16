package basic.operators;


import basic.Param;
import basic.visitors.Visitor;
import channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.*;

/**
 * operator类
 *
 * @author 陈齐翔，杜清华
 * @version 1.0
 * @since 2020/7/6 11:39
 */
public class Operator implements Visitable, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Operator.class);
    private UUID uuid;
    private String operatorID; // Operator ID
    private String operatorName; // Operator Name
    private OperatorKind operatorKind; // Operator Kind
    private Map<String, OperatorEntity> entities = new HashMap<>(); // Operator的所有实现
    private OperatorEntity selectedEntity = null; // 当前Operator选择的最优的平台实现

    // private List<Channel> inputChannels;
    private Map<String, Param> inputParamList; // 输入参数列表
    private Map<String, Param> inputDataList; // 输入数据列表

    // 记录下一跳Opt.
    // private List<Channel> outputChannels; // 这里Channel的index应该没什么用
    private Map<String, Param> outputDataList; // 有一个result就得有一个output channel，两个变量的index要（隐性）同步

    /**
     * 应避免直接创建Operator，而是使用OperatorFactory的 createOperator 或 createOperatorFromFile
     *
     * @param id
     * @param name
     * @param kind
     */
    public Operator(String id, String name, String kind) {
        this.uuid = UUID.randomUUID();
        this.operatorID = id;
        this.operatorName = name;
        switch (kind) {
            case "calculator":
                this.operatorKind = OperatorKind.CALCULATOR;
                break;
            case "supplier":
                this.operatorKind = OperatorKind.SUPPLIER;
                break;
            case "consumer":
                this.operatorKind = OperatorKind.CONSUMER;
                break;
            case "transformer":
                this.operatorKind = OperatorKind.TRANSFORMER;
                break;
            case "shuffler":
                this.operatorKind = OperatorKind.SHUFFLER;
                break;
            default:
                this.operatorKind = OperatorKind.CALCULATOR;
        }
        this.inputParamList = new HashMap<>();
        this.outputDataList = new HashMap<>();
//        this.outputChannels = new ArrayList<>();
//        this.inputChannels = new ArrayList<>();
        this.inputDataList = new HashMap<>();
    }

    /**
     * 根据传入的entity_id为当前opt设置其对应的要运行的平台，并加载平台特有的参数
     *
     * @param entityId 特定平台的ID
     * @throws FileNotFoundException 表示当前opt就没有与entityId对应的entity（平台），即该opt还不支持 entityId代表的平台
     */
    public void selectEntity(String entityId) throws FileNotFoundException {
        if (this.entities.containsKey(entityId)) {
            this.selectedEntity = this.entities.get(entityId);
            // 加载平台特有的参数
            for (Param param : this.selectedEntity.params) {
                this.addParameter(param);
            }
        } else {
            throw new FileNotFoundException("未找到与 %s 匹配的实体，请使用配置文件中platform的ID属性");
        }
    }

    /**
     * 设置Operator的实现平台属性
     *
     * @param entity 平台对象，OperatorEntity
     */
    public void addOperatorEntity(OperatorEntity entity) {
        this.entities.put(entity.entityID, entity);
    }

    /**
     * 由用户直接为Opt指定具体计算平台，而不用系统择优选择
     *
     * @param entityId
     * @throws FileNotFoundException
     */
    public void withTargetPlatform(String entityId) throws FileNotFoundException {
        this.selectEntity(entityId);
    }

    public boolean evaluate() {
        // 1. 准备数据
        // tempPrepareData();

        // 2. 检查是否获得了全部输入数据，是：继续执行； 否：return 特殊值
        for (Map.Entry entry : this.inputParamList.entrySet()) {
            Param param = (Param) entry.getValue(); // todo 要不要抛异常 "未设置参数xxx的值"
            if (!param.hasValue()) {
                return false;
            }
        }
        // 已拿到所有输入数据, 开始"计算"
        this.tempDoEvaluate();
        return true;
    }

    public void tempDoEvaluate() {
        this.logging(this.getOperatorID() + " evaluate: {\n   inputs: ");
        for (String key : this.inputParamList.keySet()) {
            this.logging("      " + key);
        }
        this.logging("   outputs:");
        for (String key : this.outputDataList.keySet()) {
            this.logging("      " + key);
        }
        this.logging("}");
    }


    /**
     * 设置opt的参数列表（非输入数据）。参数可以没有值，此时只保存其key
     *
     * @param param 参数，类型为内置的Param
     */
    public void addParameter(Param param) {
        this.inputParamList.put(param.getName(), param);
    }

    /**
     * 设置输入参数的值，用于某些参数没有默认值，需在代码中设置时使用
     *
     * @param key   Param参数的name字段
     * @param value 参数对应的值 todo 类型泛化
     */
    public void setParamValue(String key, String value) {
        if (this.inputParamList.containsKey(key)) {
            this.inputParamList.get(key).setValue(value);
        } else {
            throw new NoSuchElementException(String.format("未在%s的配置文件中找到指定的参数名：%s", this.operatorName, key));
        }
    }

    /**
     * 设置输入数据的Key，用于具有多输入/输出的opt 进行不同数据来源的映射
     *
     * @param param 输入数据的：Key, DataType
     */
    public void addInputData(Param param) {
        this.inputDataList.put(param.getName(), param);
    }

    /**
     * 设置输出数据的Key，用于具有多输入/输出的opt 进行不同数据来源的映射
     *
     * @param param 输出数据的：Key, DataType
     */
    public void addOutputData(Param param) {
        this.outputDataList.put(param.getName(), param);
    }

//    public List<Channel> getOutputChannel() {
//        return outputChannels;
//    }
//
//    public List<Channel> getInputChannel() {
//        return inputChannels;
//    }

    public Map<String, Param> getInputParamList() {
        return this.inputParamList;
    }

    public Map<String, Param> getInputDataList() {
        return this.inputDataList;
    }

    public Map<String, Param> getOutputDataList() {
        return this.outputDataList;
    }

    public boolean isLoaded() {
        return !this.entities.isEmpty(); // 用这个判断可能不太好，也许可以试试判断有没有configFile
    }

    public String getOperatorName() {
        return this.operatorName;
    }

    public String getOperatorID() {
        return this.operatorID;
    }

    public Map<String, OperatorEntity> getEntities() {
        return entities;
    }

    private void logging(String s) {
        LOGGER.info(s);
    }

    public OperatorEntity getSelectedEntities() {
        return selectedEntity;
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return operatorID;
//        return "Operator{"
//                + ", ID='" + operatorID + '\''
//                + ", name='" + operatorName + '\''
//                + ", kind=" + operatorKind
//                + ", entities=" + entities
//                + '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operator operator = (Operator) o;
        return uuid.equals(operator.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    public enum OperatorKind {
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

}
