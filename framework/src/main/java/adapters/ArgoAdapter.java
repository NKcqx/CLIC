
package adapters;

import basic.operators.Operator;
import basic.platforms.PlatformFactory;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import fdu.daslab.backend.executor.model.OperatorAdapter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 *
 * @author 杜清华
 * @since  2020/7/6 11:39
 * @version 1.0
 *
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
            return argoNodeList;
        }
        //先将operator进行分组
        for (Object o : operators) {
            Operator op = (Operator) o;
            String platform = op.getSelectedEntities().getEntityID(); //获取选择的最优的platform
            if ((!platform.equals(preplatform)) && (preplatform != null)) {
                //当前opt与之前opt的平台不同，所以先将前面的组合起来
                operatorGroups.add(operatorGroup);
                operatorGroup = new ArrayList<>();
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
            dependencies = new ArrayList<>();
            argoNodeList.add(node);
        }
        return argoNodeList;
    }

    /**
     * 读取xml配置，获取所有已知的image配置
     *
     * @return 系统所有已知的image配置
     */
    @Override
    public List<ImageTemplate> generateTemplateByConfig() {
        List<ImageTemplate> templates = new ArrayList<>();
        for (String platform : PlatformFactory.getAllPlatform()) {
            // 设置需要的配置信息到ImageTemplate中
            ImageTemplate template = new ImageTemplate();
            template.setPlatform(platform);
            Map<String, Object> platformConfig = PlatformFactory.getConfigByPlatformName(platform);
            template.setImage((String) platformConfig.get("dockerImage"));
            template.setCommand(Arrays.asList(((String) platformConfig.get("environment")).split(" ")));
            // 把executor和所有的arg按照空格拼装在一起构成运行的命令
            String executor = (String) platformConfig.get("executor");
            @SuppressWarnings("unchecked")
            String args = StringUtils.join(((Map<String, String>) platformConfig.get("args")).values(), " ");
            template.setParamPrefix(executor + " " + args);
            templates.add(template);
        }
        return templates;
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
            if (platform == null) {
                platform = operator.getSelectedEntities().getEntityID();
            }
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
        // 前期串行，分组内所有的参数放到一起为了按照steps串行，以及各个node之间的数据传递(通过文件传递)，在每个node中加上source
        // 和 sink 算子，分别用于读取和保存各个node的输入和输出。如果operator的开始算子不是source，则添加
        if (!operators.get(0).getOperatorName().equals("SourceOperator")) {
            params.add(new ArgoNode.Parameter("--operator", "SourceOperator"));
            params.add(new ArgoNode.Parameter("--input", "data/" + (id - 1) + ".csv"));
            params.add(new ArgoNode.Parameter("--separator", ","));
        }
        operators.forEach(operator -> {
            params.add(new ArgoNode.Parameter("--operator", operator.getOperatorName()));
            operator.getInputDataList().forEach((paramName, paramVal) -> {
                params.add(new ArgoNode.Parameter("--" + paramName, paramVal.getData()));
            });
        });
        if (!operators.get(operators.size() - 1).getOperatorName().equals("SinkOperator")) {
            params.add(new ArgoNode.Parameter("--operator", "SinkOperator"));
            params.add(new ArgoNode.Parameter("--output", "data/" + id + ".csv"));
            params.add(new ArgoNode.Parameter("--separator", ","));
        }
        node.setParameters(params);
        return node;
    }
}
