package fdu.daslab.backend.executor.model;

import java.util.List;

/**
 * 将平台的operator适配到argo的节点上
 */
public interface OperatorAdapter {

    /**
     * 将operator分组，连续的放在一个Argo中，并标记其继承逻辑和参数列表
     * @param operators 所有的operator
     * @return 分组后的operator
     */
    List<ArgoNode> groupContinuousOperator(List<?> operators);

}
