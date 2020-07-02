package adapters;

import basic.Operators.Operator;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.OperatorAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 */
public class ArgoAdapter implements OperatorAdapter {

    /**
     * 先简单实现，不考虑跨平台，也就是最后只会生成一个node TODO: 按照连续的平台分组
     * @param operators 所有的operator
     * @return argoNode
     */
    @Override
    public List<ArgoNode> groupContinuousOperator(List<?> operators) {
        ArgoNode node = new ArgoNode("test_case", "java", null, null);
        List<ArgoNode.Parameter> params = new ArrayList<>();
        // 前期串行，所有的参数放到一起
        operators.forEach(operator -> {
            Operator op = (Operator)operator;
            params.add(new ArgoNode.Parameter("--operator", op.getName()));
            op.getInput_data_list().forEach((paramName, paramVal) -> {
                params.add(new ArgoNode.Parameter("--" + paramName, paramVal.getData()));
            });
        });
        node.setParameters(params);
        List<ArgoNode> result = new ArrayList<>();
        result.add(node);
        return result;
    }
}
