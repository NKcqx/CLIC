package basic.operators;

import basic.Param;

import java.io.Serializable;
import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/29 2:59 下午
 */
public class OperatorEntity implements Serializable {
    String entityID;
    String entityName;
    String language;
    Double cost;
    List<Param> params;

    public OperatorEntity(String entityID, String entityName, String language, Double cost, List<Param> params) {
        this.entityID = entityID;
        this.entityName = entityName;
        this.language = language;
        this.cost = cost;
        this.params = params;
    }

    public List<Param> getParams() {
        return this.params;
    }

    public boolean hasParam(String key) {
        for (Param param : params) {
            if (param.getName().equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean setParam(String key, String value) {
        for (Param param : params) {
            if (param.getName().equals(key)) {
                param.setValue(value);
                return true;
            }
        }
        return false;
    }

    public String getEntityID() {
        return entityID;
    }

    public void setEntityID(String entityID) {
        this.entityID = entityID;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Double getCost() {
        return cost;
    }

    public void setCost(Double cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "OperatorEntity{"
                + "language='" + language + '\''
                + ", cost=" + cost
                + '}';
    }
}
