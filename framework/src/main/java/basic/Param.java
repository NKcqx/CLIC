
package basic;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Param {
    private String name;
    private ParamKind type;
    private String value = null; // 暂定String类型，之后用Object？总觉得不是这么做泛化的(或者就是Obj，反正每个Opt知道自己想用什么格式的数据)
    private Boolean isRequired = false;

    public Param(String name) {
        this(name, "string", false, null);
    }

    public Param(String name, String type) {
        this(name, type, false, null);
    }

    public Param(String name, String type, Boolean isRequired) {
        this(name, type, false, null);
    }

    public Param(String name, String type, Boolean isRequired, String value) {
        this.name = name;
        switch (type) {
            case "list":
                this.type = ParamKind.LIST;
                break;
            case "function":
                this.type = ParamKind.FUNCTION;
                break;
            case "string":
                this.type = ParamKind.STRING;
                break;
            default:
                this.type = ParamKind.STRING;
                break;
        }
        this.value = value;
        this.isRequired = isRequired;
    }

    public String getName() {
        return name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getData() {
        return this.value;
    }

    public boolean hasValue() {
        return this.value != null;
    }

    public Map<String, String> getKVData() {
        Map<String, String> data = new HashMap<>();
        data.put(this.name, this.value);
        return data;
    }

    public enum ParamKind {
        LIST, FUNCTION, STRING
    }

}
