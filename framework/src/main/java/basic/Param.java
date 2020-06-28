package basic;

import java.util.HashMap;
import java.util.Map;

public class Param {
    private String name;
    private ParamKind type;
    private String value = null; // 暂定String类型，之后用Object？总觉得不是这么做泛化的(或者就是Obj，反正每个Opt知道自己想用什么格式的数据)
    public enum ParamKind{
        LIST, FUNCTION, STRING
    }

    public Param(String name){
        this(name, "string", null);
    }

    public Param(String name, String type){
        this(name, type, null);
    }

    public Param(String name, String type, String value){
        this.name = name;
        switch (type){
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
    }

    public String getName() {
        return name;
    }

    public void setValue(String value){
        this.value = value;
    }

    public String getData(){
        return this.value;
    }

    public boolean hasValue(){
        return this.value != null;
    }

    public Map<String, String> getKVData(){
        Map<String, String> data =  new HashMap<>();
        data.put(this.name, this.value);
        return data;
    }

}
