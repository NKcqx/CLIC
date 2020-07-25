package basic;

import basic.operators.Operator;
import basic.operators.OperatorFactory;

import java.io.Serializable;

/**
 * sub-plan，保存Logical/Physical Plan的片段，用于Assign到不同平台。
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/20 3:27 下午
 */
public class Stage implements Serializable {
    private Operator headOpt;
    private Operator tailOpt;
    private String id;
    private String name;
    private String platform;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getPlatform() {
        return platform;
    }

    public Stage(String id, String platform) {
        this.id = id;
        this.platform = platform;
    }

    public void setHead(Operator head){
        this.headOpt = head;
    }

    public void setTail(Operator tail){
        this.tailOpt = tail;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public boolean isEnd(Operator opt){
        return tailOpt == opt;
    }

    public Operator getHeadOpt(){
        return headOpt;
    }
}
