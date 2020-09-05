package basic.operators;

import basic.Param;

import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/29 2:59 下午
 */
public class OperatorEntity {
    String entityID;
    String language;
    Double cost;
    List<Param> params;
    public OperatorEntity(String entityID, String language, Double cost, List<Param> params) {
        this.entityID = entityID;
        this.language = language;
        this.cost = cost;
        this.params = params;
    }

    public String getEntityID() {
        return entityID;
    }

    public void setEntityID(String entityID) {
        this.entityID = entityID;
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
