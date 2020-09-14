package fdu.daslab.executable.basic.model;

import java.io.Serializable;

/**
 * 作用和Channel大致一样，但功能更少，只是为了用Key链接两个Opt
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 5:30 下午
 */
public class Connection implements Serializable {
    private OperatorBase sourceOpt;
    private String sourceKey;
    private OperatorBase targetOpt;
    private String targetKey;

    public Connection(OperatorBase sourceOpt, String sourceKey, OperatorBase targetOpt, String targetKey) {
        this.sourceOpt = sourceOpt;
        this.sourceKey = sourceKey;
        this.targetOpt = targetOpt;
        this.targetKey = targetKey;
    }

    public OperatorBase getSourceOpt() {
        return sourceOpt;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public OperatorBase getTargetOpt() {
        return targetOpt;
    }

    public String getTargetKey() {
        return targetKey;
    }
}
