package adapters;

import basic.Param;
import basic.Stage;
import basic.Util;
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
import org.jgrapht.graph.DefaultListenableGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import siamese.SiameseAdapter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * 将平台内部的operator，按照连续的平台分为一组，并组装成argo的形式
 *
 * @author 杜清华，陈齐翔，刘丰艺
 * @version 1.0
 * @since 2020/7/6 11:39
 */
public class ArgoAdapter implements OperatorAdapter {

    // 记录已添加到yaml中的算子
    public static List<Operator> opts = new ArrayList<>();

    @Override
    public List<ArgoNode> adaptOperator(List<?> operators) {
        // List<Stage> stages =  wrapWithHeadTail((List<Stage> operators));
        return ArgoAdapter.setArgoNode((List<Stage>) operators);
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
    public static List<ArgoNode> setArgoNode(List<Stage> stages) {
        List<ArgoNode> argoNodeList = new ArrayList<>();
        ArgoNode dependencyNode = null;
        // 1. 遍历stage里的dag，生成ArgoNode
        for (Stage stage : stages) {
            // todo dependency是上一个ArgoNode
            String id = Util.generateID();
            ArgoNode argoNode = null;
            if (dependencyNode != null) {
                ArrayList<ArgoNode> dependencies = new ArrayList<>();
                dependencies.add(dependencyNode); // todo 之后会有不止一个dependency
                argoNode = new ArgoNode(id, stage.getName(), stage.getPlatform(), dependencies);
            } else {
                argoNode = new ArgoNode(id, stage.getName(), stage.getPlatform(), null);
            }

            // 遍历 子DAG，把所有opt转为Map保存
            AsSubgraph<Operator, Channel> subGraph = stage.getGraph();
            Map<String, Object> yamlMap = ArgoAdapter.graph2Yaml(stage.getGraph());
            try {
                String path = YamlUtil.getResPltDagPath() + "physical-dag-" + argoNode.getId() + ".yml";
                YamlUtil.writeYaml(new OutputStreamWriter((new FileOutputStream(path))), yamlMap);
                argoNode.setParameters(new HashMap<String, String>() {{
                    put("--dagPath", path);
                }});
                argoNodeList.add(argoNode);
                dependencyNode = argoNode;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
        return argoNodeList;
    }

    public static Map<String, Object> graph2Yaml(Graph<Operator, Channel> graph) {
        // 遍历stage里的dag, 转成YAML字符串
        List<Map<String, Object>> optMapList = new ArrayList<>(); // "operators"字段，是stage里所有opt的列表 YML列表
        List<Map<String, Object>> dagList = new ArrayList<>(); // "dag"字段，各个边的列表

        // 遍历 子DAG，把所有opt转为Map保存
        // 其实没必要按序添加，只要边的方向搞对后端就能重构出来
        BreadthFirstIterator<Operator, Channel> breadthFirstIterator = new BreadthFirstIterator<>(graph);

        while (breadthFirstIterator.hasNext()) {
            Operator curOpt = breadthFirstIterator.next();
            fillLists(optMapList, dagList, curOpt, graph);
        }

        // 如果遇到与Siamese组对接的情况
        // 则需要把之前已添加到yaml中的TableSource算子删除，因为到时候返回的树自带Relation算子
        removeTableSource(optMapList, dagList);

        return new HashMap<String, Object>() {{
            put("operators", optMapList);
            put("dag", dagList);
        }};
    }

    /**
     * 填充两个List的内容
     * @param optMapList
     * @param dagList
     * @param curOpt
     * @param graph
     */
    private static void fillLists(List<Map<String, Object>> optMapList, List<Map<String, Object>> dagList,
                                  Operator curOpt, Graph<Operator, Channel> graph) {
        String param = null;

        // param不为空，代表当前算子是要使用优化SQL语句功能的query算子
        param = checkOptOfSiamese(curOpt);

        if (param != null) {
            // 与Siamese进行对接
            dockingWithSiamese(param, curOpt, optMapList, dagList, graph);
        } else {
            // 普通情况
            if (!opts.contains(curOpt)) {
                // 之前已添加过到yaml文件中的算子，就不再重复添加了
                // 这是因为假如上一个算子是使用优化SQL功能的query算子
                // 那么它的下一跳算子（假设为sink）也会添加到由query拆分成的新DAG，此时sink算子的上一跳就变成树的根节点算子而不是query
                // 然而在外面一层，遍历完query算子之后，还会再次遍历到sink，此时的sink算子记录的上一跳仍是原query算子
                // 为了保证optMapList中的sink算子信息在再次遍历时不被覆盖，需要一个列表给已遍历过的算子作标记
                opts.add(curOpt);
                optMapList.add(ArgoAdapter.operator2Map(curOpt)); // 先把 opt -> map 为了生成yaml
                dagList.add(ArgoAdapter.operatorDependency2Map(graph, curOpt)); // channel -> map
            }
        }

        // 这个函数在原代码基础上添加了几个if条件分支判断，这是因为Siamese组的对接，需要将一个query算子拆分、生成成一个新的DAG
        // 而graph是AsSubgraph类型，无法给它添加新节点和边
    }

    /**
     * 判断该DAG中是否有要与Siamese对接的query算子
     * 有的话，需要将这个算子展开成一个子DAG，再写进yaml文件中
     * 这里得用硬编码，没想到更好的解决办法，对接的锅
     * @param opt
     * @return
     */
    private static String checkOptOfSiamese(Operator opt) {
        if (opt.getOperatorName().equals("QueryOperator")) {
            Map<String, Param> paramsMap = opt.getInputParamList();
            for (Map.Entry<String, Param> entry : paramsMap.entrySet()) {
                // 如果sqlNeedForOptimized参数不为空，则表明这个算子是需要优化SQL语句的query算子
                if (entry.getKey().equals("sqlNeedForOptimized") && entry.getValue().getData() != null) {
                    return entry.getValue().getData();
                }
            }
        }
        return null;
    }

    /**
     * 如果遇到需要与Siamese对接的情况（遇到含有需要优化SQL语句的参数的query算子）
     * 则执行这个对接函数
     * @param sqlText
     * @param curOpt
     * @param optMapList
     * @param dagList
     * @param graph
     */
    private static void dockingWithSiamese(String sqlText, Operator curOpt,
                                      List<Map<String, Object>> optMapList, List<Map<String, Object>> dagList,
                                      Graph<Operator, Channel> graph) {

        // 获取query算子下一跳的所有算子，需要将它们与CLIC根据Siamese的tree生成的子DAG的输出节点连起来
        // 实际上query算子的下一跳通常只有一个算子（起码对目前的CLIC来说）
        Set<Channel> outputEdges = graph.outgoingEdgesOf(curOpt);
        Operator targetOpt = null;
        for (Channel outputEdge : outputEdges) {
            targetOpt = graph.getEdgeTarget(outputEdge);
        }
        // 做标记，yaml中已添加过这个算子
        opts.add(targetOpt);

        try {
            // 根据Siamese返回的树，我们创建一个DAG（原来的graph参数是Subgraph类型，无法添加顶点）
            DefaultListenableGraph<Operator, Channel> sqlGraph =
                    new DefaultListenableGraph<>(new SimpleDirectedWeightedGraph<>(Channel.class));

            List<Operator> qOpts = new ArrayList<>();
            // 进行对接
            qOpts = SiameseAdapter.unfoldQuery2DAG(sqlText, targetOpt, sqlGraph);
            // 解析树生成的子DAG中的算子都加到yaml文件中
            for (Operator qOpt : qOpts) {
                optMapList.add(ArgoAdapter.operator2Map(qOpt));
                dagList.add(ArgoAdapter.operatorDependency2Map(sqlGraph, qOpt));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断yaml中是否有要删除的算子信息
     * 因为生成的新DAG里已经有从数据源中读取table的Relation算子
     * 因此需要将之前在外面遍历过、已经存在optMapList中的TableSource算子删除
     * 为了对接，只能用硬编码，暂时没想到好办法
     * @param optMapList
     * @param dagList
     */
    private static void removeTableSource(List<Map<String, Object>> optMapList, List<Map<String, Object>> dagList) {
        boolean dockingFlag = false;
        // 先判断是否遇到要使用优化SQL语句的query算子的情况
        for (int i = 0; i<optMapList.size(); i++) {
            if (optMapList.get(i).get("name").equals("TRelationOperator")) {
                dockingFlag = true;
                break;
            }
        }
        // 如果确定使用了对接功能
        if (dockingFlag) {
            for (int i = 0; i<optMapList.size(); i++) {
                if (optMapList.get(i).get("name").equals("TableSourceOperator")) {
                    optMapList.remove(i);
                    // List会动态变化，因此索引后退一次
                    i--;
                }
            }
            for (int i = 0; i<dagList.size(); i++) {
                if (((String) dagList.get(i).get("id")).contains("TableSourceOperator")) {
                    dagList.remove(i);
                    i--;
                }
            }
        }
    }

    /**
     * 把一个Operator中的各个属性转化为Map，用于最后将Opt生成YAML
     *
     * @param opt 要转换的Operator
     * @return Key-Value格式的Map, 其中 Key为参数名，Value为参数值
     */
    private static Map<String, Object> operator2Map(Operator opt) {
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
     *
     * @param graph 基础Graph，用于
     * @param opt   当前要解析的Opt
     * @return 当前Opt 的 Map格式 的依赖关系, 其实就是将Opt的各个Channel
     */
    private static Map<String, Object> operatorDependency2Map(Graph graph, Operator opt) {
        Map<String, Object> dependencyMap = new HashMap<>(); // 当前Opt的依赖对象
        dependencyMap.put("id", opt.getOperatorID());

        if (graph.inDegreeOf(opt) != 0) { // head Operator
            Set<Channel> inputChannels = graph.incomingEdgesOf(opt); // dependency channels
            List<Map<String, String>> dependencies = new ArrayList<>(); // denpendencies字段，是一个List<Map> 每个元素是其中一个依赖
            for (Channel channel : inputChannels) {
                dependencies.add(new HashMap<String, String>() {{
                    put("id", ((Operator) graph.getEdgeSource(channel)).getOperatorID());
                    put("sourceKey", channel.getKeyPair().getValue0());
                    put("targetKey", channel.getKeyPair().getValue1());
                }});
            }
            dependencyMap.put("dependencies", dependencies);
        }
        return dependencyMap;
    }
}
