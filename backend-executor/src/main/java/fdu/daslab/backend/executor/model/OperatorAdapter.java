package fdu.daslab.backend.executor.model;

import java.util.List;

/**
 * 将平台的operator适配到argo的节点上
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:56 PM
 */
public interface OperatorAdapter {

    //    放到Visitor里做
//    /**
//     * 将operator分组，连续的放在一个Argo中，并标记其继承逻辑和参数列表
//     * @param operators 所有的operator
//     * @return 分组后的operator
//     */
//    List<ArgoNode> groupContinuousOperator(List<?> operators);
    List<ArgoNode> adaptOperator(List<?> operators);

    /**
     * 平台需要根据配置信息，组装成ImageTemplate传入
     *
     * @return 各个平台的ImageTemplate实体
     */
    List<ImageTemplate> generateTemplateByConfig();
}
