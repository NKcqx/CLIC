package adapters;

import basic.Param;
import basic.Stage;
import basic.operators.Operator;
import basic.platforms.PlatformFactory;
import basic.traversal.BfsTraversal;
import channel.Channel;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import fdu.daslab.backend.executor.model.OperatorAdapter;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 *
 * @author 杜清华，陈齐翔
 * @since  2020/7/6 11:39
 * @version 1.0
 *
 */
public class ArgoAdapter implements OperatorAdapter {

//    /**
//     * 将operator进行分组，分组规则按照连接且platform相同的算子归到一个node（目前适用于steps式串行）
//     *
//     * @param operators 所有的operator
//     * @return argoNode
//     */
//    @Override
//    public List<ArgoNode> groupContinuousOperator(List<?> operators) {
//
//        List<List<Operator>> operatorGroups = new ArrayList<>();  //operator根据平台情况进行分组
//        List<ArgoNode> argoNodeList = new ArrayList<>();   //根据分组情况设定node
//        ArgoNode node = null;
//        String preplatform = null;
//        List<Operator> operatorGroup = new ArrayList<>(); //划分到一组的operator
//        List<ArgoNode> dependencies = new ArrayList<>();
//        int idInJob = 0; //记录node的id作为标记（在同一个job下可用）
//
//        if (operators.isEmpty()) {
//            return argoNodeList;
//        }
//        //先将operator进行分组
//        for (Object o : operators) {
//            Operator op = (Operator) o;
//            String platform = op.getSelectedEntities().getEntityID(); //获取选择的最优的platform
//            if ((!platform.equals(preplatform)) && (preplatform != null)) {
//                //当前opt与之前opt的平台不同，所以先将前面的组合起来
//                operatorGroups.add(operatorGroup);
//                operatorGroup = new ArrayList<>();
//            }
//
//            operatorGroup.add(op);
//            preplatform = platform;
//            if (o == operators.get(operators.size() - 1)) {
//                operatorGroups.add(operatorGroup);
//            }
//        }
//        //遍历每个分组，并设置arg node
//        for (List<Operator> plt : operatorGroups) {
//            idInJob += 1;
//            //当前是串行，所以暂定传入的dependencies是前一个node
//            dependencies.add(node);
//            node = setArgoNode(plt, idInJob, dependencies); //为分组设置Argo node
//            dependencies = new ArrayList<>();
//            argoNodeList.add(node);
//        }
//        return argoNodeList;
//    }

    @Override
    public List<ArgoNode> adaptOperator(List<?> operators) {
        // List<Stage> stages =  wrapWithHeadTail((List<Stage> operators));
        return setArgoNode((List<Stage>) operators);
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
//
//    /**
//     * 对每个分组设置相应的Argo node
//     *
//     * @param operators    分组平台下的所有operator
//     * @param id           该分组node在该job下的id
//     * @param dependencies 该分组node所依赖的其他node关系（无循环）
//     * @return argoNode
//     */
//    public ArgoNode setArgoNode(List<Operator> operators, int id, List<ArgoNode> dependencies) {
//        List<ArgoNode.Parameter> params = new ArrayList<>();
//        int randomNumber = (int) Math.round(Math.random() * 1000); //取[0,1000]的随机整数
//        String platform = null;
//        List<String> optName = new ArrayList<>();
//        for (Operator operator : operators) {
//            if (platform == null) {
//                platform = operator.getSelectedEntities().getEntityID();
//            }
//            optName.add(operator.getOperatorName());
//        }
//        String nameSub = "";
//        //设置argo node的name
//        if (optName.size() > 3) {
//            int i = 0;
//            for (String n : optName) {
//                i++;
//                nameSub = nameSub + n.substring(0, 2);
//                if (i > 5) {
//                    break; // 最多取前六个name
//                }
//            }
//        } else {
//            for (String n : optName) {
//                nameSub = nameSub + n.replace("Operator", "");
//            }
//        }
//        String name = "Job-" + platform + "-" + nameSub + randomNumber + "-" + id;
//        ArgoNode node = new ArgoNode(id, name, platform, dependencies, null);
//        // 前期串行，分组内所有的参数放到一起为了按照steps串行，以及各个node之间的数据传递(通过文件传递)，在每个node中加上source
//        // 和 sink 算子，分别用于读取和保存各个node的输入和输出。如果operator的开始算子不是source，则添加
//        if (!operators.get(0).getOperatorName().equals("SourceOperator")) {
//            params.add(new ArgoNode.Parameter("--operator", "SourceOperator"));
//            params.add(new ArgoNode.Parameter("--input", "data/" + (id - 1) + ".csv"));
//            params.add(new ArgoNode.Parameter("--separator", ","));
//        }
//        operators.forEach(operator -> {
//            params.add(new ArgoNode.Parameter("--operator", operator.getOperatorName()));
//            operator.getInputDataList().forEach((paramName, paramVal) -> {
//                params.add(new ArgoNode.Parameter("--" + paramName, paramVal.getData()));
//            });
//        });
//        if (!operators.get(operators.size() - 1).getOperatorName().equals("SinkOperator")) {
//            params.add(new ArgoNode.Parameter("--operator", "SinkOperator"));
//            params.add(new ArgoNode.Parameter("--output", "data/" + id + ".csv"));
//            params.add(new ArgoNode.Parameter("--separator", ","));
//        }
//        node.setParameters(params);
//        return node;
//    }


    public List<ArgoNode> setArgoNode(List<Stage> stages){
        List<ArgoNode> argoNodeList = new ArrayList<>();
        // 1. 遍历stage里的dag，生成ArgoNode
        for (Stage stage : stages){
            // todo id好好生成下, dependency是上一个ArgoNode
            ArgoNode argoNode = new ArgoNode(0, stage.getName(), stage.getPlatform(), null);
            // 遍历stage里的dag, 转成YAML字符串
            List<Map<String, Object>> optMapList = new ArrayList<>(); // "objects"字段，是stage里所有opt的列表 YML列表
            List<Map<String, Object>> dagList = new ArrayList<>(); // "dag"字段，各个边的列表

            Operator stageRootOpt = stage.getHead();
            BfsTraversal bfsTraversal = new BfsTraversal(stageRootOpt);
            while (bfsTraversal.hasNextOpt()){
                Operator curOpt = bfsTraversal.nextOpt();
                optMapList.add(operator2Map(curOpt)); // 先把 opt -> map 为了生成yaml
                dagList.add(operatorDependency2Map(curOpt, curOpt == stageRootOpt)); // channel -> map
                if  (curOpt == stage.getTail()){
                    break;
                }
            }

            argoNode.setParameters(new HashMap<String, Object>(){{
                put("operators", optMapList);
                put("dag", dagList);
            }});
            argoNodeList.add(argoNode);
        }
        return argoNodeList;
    }

    private Map<String, Object> operator2Map(Operator opt){
        /*Map<String, String> paramsList = new ArrayList<>();
        for (Param param : opt.getInputDataList().values()){
            paramsList.add(param.getKVData());
        }*/
        Map<String, Param> paramsList =  opt.getInputParamList();
        Map<String, Param> inputDataList = opt.getInputDataList();
        Map<String, Param> outputDataList = opt.getOutputDataList();

        Map<String, String> paramsMap = new HashMap<>();
        paramsList.forEach((s, param) -> paramsMap.putAll(param.getKVData()));
        Map<String, Object> optMap = new HashMap<String, Object>(){{
            put("name", opt.getOperatorName());
            put("id", opt.getOperatorID());
            put("params", paramsMap);
            put("inputKeys", inputDataList.keySet().toArray());
            put("outputKeys", outputDataList.keySet().toArray());
        }};
        return  optMap;
    }

    private Map<String, Object> operatorDependency2Map(Operator opt, boolean isHead){
        Map<String, Object> dependencyMap = new HashMap<>(); // 当前Opt的依赖对象
        dependencyMap.put("name", opt.getOperatorName());
        if (!isHead){
            List<Channel> inputChannels =  opt.getInputChannel(); // todo 这该不该直接拿Channel呢
            List<Map<String, String>> dependencies = new ArrayList<>(); // denpendencies字段，是一个List<Map> 每个元素是其中一个依赖
            for (Channel channel : inputChannels){
                dependencies.add(new HashMap<String, String>(){{
                    put("id", channel.getSourceOperator().getOperatorID());
                    put("sourceKey", channel.getKeyPair().getKey());
                    put("targetKey", channel.getKeyPair().getValue());
                }});
            }
            dependencyMap.put("dependencies", dependencies);
        }
        return dependencyMap;
    }
}
