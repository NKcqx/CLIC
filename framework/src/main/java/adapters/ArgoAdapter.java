package adapters;

import basic.Param;
import basic.Stage;
import basic.operators.Operator;
import basic.platforms.PlatformFactory;
import channel.Channel;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import fdu.daslab.backend.executor.model.OperatorAdapter;
import fdu.daslab.backend.executor.utils.YamlUtil;
import org.apache.commons.lang3.StringUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import java.util.*;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 *
 * @author 杜清华，陈齐翔
 * @version 1.0
 * @since 2020/7/6 11:39
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

    /**
     * 把Stage的列表转换为多个ArgoNode的列表，用于生成YAML
     *
     * @param stages 所有的Stage
     * @return 所有的ArgoNode
     */
    public List<ArgoNode> setArgoNode(List<Stage> stages) {
        List<ArgoNode> argoNodeList = new ArrayList<>();
        ArgoNode dependencyNode = null;
        // 1. 遍历stage里的dag，生成ArgoNode
        for (Stage stage : stages) {
            // todo id好好生成下(ID Supplier), dependency是上一个ArgoNode
            int id = new Date().hashCode();
            ArgoNode argoNode = null;
            if (dependencyNode != null){
                ArrayList<ArgoNode> dependencies = new ArrayList<>();
                dependencies.add(dependencyNode); // todo 之后会有不止一个dependency
                argoNode = new ArgoNode(id, stage.getName(), stage.getPlatform(),  dependencies);
            }else {
                argoNode = new ArgoNode(id, stage.getName(), stage.getPlatform(),  null);
            }

            // 遍历stage里的dag, 转成YAML字符串
            List<Map<String, Object>> optMapList = new ArrayList<>(); // "operators"字段，是stage里所有opt的列表 YML列表
            List<Map<String, Object>> dagList = new ArrayList<>(); // "dag"字段，各个边的列表

            // 遍历 子DAG，把所有opt转为Map保存
            AsSubgraph<Operator, Channel> subGraph = stage.getGraph();
            // 其实没必要按序添加，只要边的方向搞对后端就能重构出来
            BreadthFirstIterator<Operator, Channel> breadthFirstIterator = new BreadthFirstIterator<>(subGraph);

            while (breadthFirstIterator.hasNext()) {
                Operator curOpt = breadthFirstIterator.next();
                optMapList.add(operator2Map(curOpt)); // 先把 opt -> map 为了生成yaml
                dagList.add(operatorDependency2Map(subGraph, curOpt)); // channel -> map

            }
            String path = YamlUtil.getResPltDagPath() + "physical-dag-" + argoNode.getId() + ".yml";
            YamlUtil.writeYaml(path, new HashMap<String, Object>() {{
                put("operators", optMapList);
                put("dag", dagList);
            }});
            argoNode.setParameters(new HashMap<String, String>() {{
                put("--dagPath", path);
            }});
            argoNodeList.add(argoNode);
            dependencyNode = argoNode;
        }
        return argoNodeList;
    }

    /**
     * 把一个Operator中的各个属性转化为Map，用于最后将Opt生成YAML
     * @param opt 要转换的Operator
     * @return Key-Value格式的Map, 其中 Key为参数名，Value为参数值
     */
    private Map<String, Object> operator2Map(Operator opt) {
        Map<String, Param> paramsList = opt.getInputParamList();
        Map<String, Param> inputDataList = opt.getInputDataList();
        Map<String, Param> outputDataList = opt.getOutputDataList();

        Map<String, String> paramsMap = new HashMap<>();
        paramsList.forEach((s, param) -> paramsMap.putAll(param.getKVData()));
        Map<String, Object> optMap = new HashMap<String, Object>() {{
            put("name", opt.getOperatorName());
            put("id", opt.getOperatorID());
            put("params", paramsMap);
            put("inputKeys", inputDataList.keySet().toArray());
            put("outputKeys", outputDataList.keySet().toArray());
        }};
        return optMap;
    }

    /**
     * 将Opt的各个Channel转为Map格式，用于创建YAML中的dag字段，该字段主要用于声明DAG节点间的依赖关系
     * @param graph 基础Graph，用于
     * @param opt 当前要解析的Opt
     * @return 当前Opt 的 Map格式 的依赖关系, 其实就是将Opt的各个Channel
     */
    private Map<String, Object> operatorDependency2Map(Graph graph, Operator opt) {
        Map<String, Object> dependencyMap = new HashMap<>(); // 当前Opt的依赖对象
        dependencyMap.put("id", opt.getOperatorID());

        if(graph.inDegreeOf(opt) != 0){ // head Operator
            Set<Channel> inputChannels = graph.incomingEdgesOf(opt); // dependency channels
            List<Map<String, String>> dependencies = new ArrayList<>(); // denpendencies字段，是一个List<Map> 每个元素是其中一个依赖
            for (Channel channel : inputChannels){
                dependencies.add(new HashMap<String, String>() {{
                    put("id", ((Operator)graph.getEdgeSource(channel)).getOperatorID());
                    put("sourceKey", channel.getKeyPair().getValue0());
                    put("targetKey", channel.getKeyPair().getValue1());
                }});
            }
            dependencyMap.put("dependencies", dependencies);
        }
        return dependencyMap;
    }
}
