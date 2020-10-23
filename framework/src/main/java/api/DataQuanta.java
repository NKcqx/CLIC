package api;

import basic.operators.Operator;
import basic.operators.OperatorFactory;


import java.io.FileNotFoundException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;


/**
 * 相当于DAG图中的每个节点，节点内部保存实际的Operator，提供一系列API以供构造下一跳（可以有多个）
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public final class DataQuanta {
    private Operator operator;

    private DataQuanta(Operator operator) {
        this.operator = operator;
    }

    /**
     * 根据ability创建一个DataQuanta，并载入（无依赖）参数值
     *
     * @param ability Operator要具有的功能
     * @param params  Operator参数的值，K-V形式，可为空；注意，这传入的参数值只能是静态的值，例如最大迭代次数、是否倒序，而不是依赖上一跳的输出
     * @return 完整的DataQuanta
     * @throws Exception 一系列错误的结合，包括XML结构解析错误、文件找不到、传入的key和配置文件里的参数名对不上等
     */
    public static DataQuanta createInstance(String ability, Map<String, String> params) throws Exception {
        if (ability.equals("empty")) {
            return null;
        } else {
            // 先创建出符合要求的operator
            Operator opt = DataQuanta.createOperator(ability);
            if (params != null) {
                // 再设置静态的输入数据
                for (Map.Entry entry : params.entrySet()) {
                    String key = (String) entry.getKey();
                    String value = (String) entry.getValue();
                    opt.setParamValue(key, value);
                }
            }
            DataQuanta dq = new DataQuanta(opt);
            return dq;
        }
    }

    public DataQuanta withTargetPlatform(String platformName) {
        try {
            this.operator.withTargetPlatform(platformName.toLowerCase().trim());
            return this;
        } catch (FileNotFoundException e) {
            throw new NoSuchElementException(
                    String.format("未为%s找到与%s匹配的平台，可用的平台有：%s",
                            this.operator.getOperatorName(),
                            platformName,
                            this.operator.getEntities().keySet().toString())
            );
        }

    }

    /**
     * 功能单一，只是为了确保只有这一个地方提到了OperatorMapping的事
     *
     * @param operatorAbility opt应该具有的功能
     * @return 包含该opt的新的DataQuanta
     * @throws Exception
     */
    private static Operator createOperator(String operatorAbility) throws Exception {
        // 先根据功能创建对应的opt
        Operator opt = OperatorFactory.createOperator(operatorAbility);
        // 封装为DataQuanta
        return opt;
    }

    /**
     * 拿到DataQuanta所代表的Operator
     *
     * @return DataQuanta所代表的Operator
     */
    public Operator getOperator() {
        return operator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataQuanta that = (DataQuanta) o;
        return getOperator().equals(that.getOperator());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOperator());
    }
}
