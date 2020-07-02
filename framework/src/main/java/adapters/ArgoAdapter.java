package adapters;

import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.OperatorAdapter;

import java.util.List;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 */
public class ArgoAdapter implements OperatorAdapter {
    @Override
    public List<ArgoNode> groupContinuousOperator(List<Object> operators) {
        return null;
    }
}
