package adapters;

import basic.operators.Operator;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.OperatorAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 */
public class ArgoAdapter implements OperatorAdapter {

    /**
     * 将operator进行分组，分组规则按照连接且platform相同的算子归到一个node（目前适用于steps式串行）
     *
     * @param operators 所有的operator
     * @return argoNode
     */
    @Override
    public List<ArgoNode> groupContinuousOperator(List<?> operators) {

        List<List<Operator>> operatorGroups = new ArrayList<>();  //operator根据平台情况进行分组
        List<ArgoNode> argoNodeList = new ArrayList<>();   //根据分组情况设定node
        ArgoNode node = null;
        String preplatform = null;
        List<Operator> operatorGroup = new ArrayList<>(); //划分到一组的operator
        List<ArgoNode> dependencies = new ArrayList<>();
        int idInJob = 0; //记录node的id作为标记（在同一个job下可用）

        if (operators.isEmpty()) {
            System.out.println("Warning: the param(operators) is empty in groupContinuousOperator function!!!");
            return argoNodeList;
        }
        //先将operator进行分组
        for (Object o : operators) {
            Operator op = (Operator) o;
            String platform = op.getSelectedEntities().getEntityID(); //获取选择的最优的platform
            if ((!platform.equals(preplatform)) && (preplatform != null)) {
                //当前opt与之前opt的平台不同，所以先将前面的组合起来
                operatorGroups.add(operatorGroup);
                operatorGroup.clear();
            }

            operatorGroup.add(op);
            preplatform = platform;
            if (o == operators.get(operators.size() - 1)) {
                operatorGroups.add(operatorGroup);
            }
        }
        //遍历每个分组，并设置arg onode
        for (List<Operator> plt : operatorGroups) {
            idInJob += 1;
            //当前是串行，所以暂定传入的dependencies是前一个node
            dependencies.add(node);
            node = setArgoNode(plt, idInJob, dependencies); //为分组设置Argo node
            argoNodeList.add(node);
        }
        return argoNodeList;
    }

    /**
     * 对每个分组设置相应的Argo node
     *
     * @param operators    分组平台下的所有operator
     * @param id           该分组node在该job下的id
     * @param dependencies 该分组node所依赖的其他node关系（无循环）
     * @return argoNode
     */
    public ArgoNode setArgoNode(List<Operator> operators, int id, List<ArgoNode> dependencies) {
        List<ArgoNode.Parameter> params = new ArrayList<>();
        int randomNumber = (int) Math.round(Math.random() * 1000); //取[0,1000]的随机整数
        String platform = null;
        List<String> optName = new ArrayList<>();
        for (Operator operator : operators) {
            //获取当前分组的platform
            if (platform == null) {
                platform = operator.getSelectedEntities().getEntityID();
            }
            //获取所有opt的name
            optName.add(operator.getOperatorName());
        }
        String nameSub = "";
        //设置argo node的name
        if (optName.size() > 3) {
            int i = 0;
            for (String n : optName) {
                i++;
                nameSub = nameSub + n.substring(0, 2);
                if (i > 5) {
                    break; // 最多取前六个name
                }
            }
        } else {
            for (String n : optName) {
                nameSub = nameSub + n.replace("Operator", "");
            }
        }
        String name = "Job-" + platform + "-" + nameSub + randomNumber + "-" + id;

        ArgoNode node = new ArgoNode(id, name, platform, dependencies, null);
        // 前期串行，分组内所有的参数放到一起
        operators.forEach(operator -> {
            params.add(new ArgoNode.Parameter("--operator", operator.getOperatorName()));
            operator.getInputDataList().forEach((paramName, paramVal) -> {
                params.add(new ArgoNode.Parameter("--" + paramName, paramVal.getData()));
            });
        });
        //设置当前node的参数
        node.setParameters(params);

        return node;
    }
}
