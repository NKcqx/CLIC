
package api;

import adapters.ArgoAdapter;
import basic.Configuration;
import basic.Stage;
import basic.Util;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.platforms.PlatformFactory;
import basic.visitors.ExecutionGenerationVisitor;
import basic.visitors.PrintVisitor;
import basic.visitors.WorkflowVisitor;
import channel.Channel;
import fdu.daslab.backend.executor.model.Workflow;
import fdu.daslab.backend.executor.utils.YamlUtil;
import org.javatuples.Pair;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultListenableGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class PlanBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanBuilder.class);
    private List<DataQuanta> headDataQuantas = new ArrayList<>(); // 其实可以有多个head
    // private SimpleDirectedWeightedGraph<Operator, Channel> graph = null;
    private DefaultListenableGraph<Operator, Channel> graph = null;
    private Configuration configuration;

    /**
     * @param configuration 配置文件，从中加载系统运行时必要的参数，即系统运行时的上下文
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public PlanBuilder(Configuration configuration) throws IOException, SAXException, ParserConfigurationException {
        this.configuration = configuration;
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
        PlatformFactory.initMapping(configuration.getProperty("platform-mapping-file"));
        this.graph = new DefaultListenableGraph<>(new SimpleDirectedWeightedGraph<>(Channel.class));
    }

    public PlanBuilder() throws ParserConfigurationException, SAXException, IOException {
        this(new Configuration());
    }

    public boolean addVertex(DataQuanta dataQuanta) {
        return this.addVertex(dataQuanta.getOperator());
    }

    public boolean addVertex(Operator operator) {
        return graph.addVertex(operator);
    }

    public boolean addEdges(DataQuanta sourceDataQuanta,
                            DataQuanta targetDataQuanta,
                            List<Pair<String, String>> keyPairs) {
        Channel channel = new Channel(keyPairs);
        return graph.addEdge(sourceDataQuanta.getOperator(), targetDataQuanta.getOperator(), channel);
    }

    public boolean addEdge(DataQuanta sourceDataQuanta,
                           DataQuanta targetDataQuanta,
                           Pair<String, String> keyPair) {
        List<Pair<String, String>> keyPairs = new ArrayList<>();
        if (keyPair == null) {
            keyPair = new Pair<>("result", "data");
        }
        keyPairs.add(keyPair);
        return this.addEdges(sourceDataQuanta, targetDataQuanta, keyPairs);
    }

    public boolean addEdge(DataQuanta sourceDataQuanta,
                           DataQuanta targetDataQuanta) {
        return this.addEdge(sourceDataQuanta, targetDataQuanta, null);
    }

    public DataQuanta readDataFrom(Map<String, String> params) throws Exception {
        DataQuanta dataQuanta = DataQuanta.createInstance("source", params);
        this.headDataQuantas.add(dataQuanta);
        return dataQuanta; // 不需要connectTo
    }

    public List<DataQuanta> getHeadDataQuanta() {
        return headDataQuantas;
    }

    public DefaultListenableGraph<Operator, Channel> getGraph() {
        return graph;
    }

    /**
     * 把 headOperator 交给各类Visitor
     * 1: Optimize the pipeline structure
     * 2: Mapping operator to Executable, Platform depended Operator
     * 3. Run
     */
    public void execute() throws Exception {
        // 在这 add 各种Listener
        LOGGER.info("===========【Stage 1】Get User Defined Plan ===========");
        this.printPlan();
        LOGGER.info("   ");

        LOGGER.info("===========【Stage 2】Choose best Operator implementation ===========");
        this.optimizePlan();
        this.printPlan();
        LOGGER.info("   ");

//        LOGGER.info("===========【Stage 3】Visualization ===========");
//        this.visualizePlan();
//        LOGGER.info("   ");

        LOGGER.info("===========【Stage 4】execute plan ==========");
        this.executePlan();

    }

    public void printPlan() {
        LOGGER.info("Current Plan:");
        TopologicalOrderIterator<Operator, Channel> topologicalOrderIterator = new TopologicalOrderIterator<>(graph);
        PrintVisitor printVisitor = new PrintVisitor();
        while (topologicalOrderIterator.hasNext()) {
            topologicalOrderIterator.next().acceptVisitor(printVisitor);
        }
    }

    public void optimizePlan() {
        TopologicalOrderIterator<Operator, Channel> topologicalOrderIterator = new TopologicalOrderIterator<>(graph);
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor();
        while (topologicalOrderIterator.hasNext()) {
            topologicalOrderIterator.next().acceptVisitor(executionGenerationVisitor);
        }
    }

    public void visualizePlan() {
        Util.visualize(graph);
    }

    private void executePlan() throws Exception {
        /**
         * 1. 调用WorkflowVisitor 得到Stages
         * 2. 创建Workflow 传入stages和ArgoAdapter（adapter使用新写的setArgoNode 接收List of Stage）
         * 3. Workflow.execute()
         */
        TopologicalOrderIterator<Operator, Channel> topologicalOrderIterator = new TopologicalOrderIterator<>(graph);
        WorkflowVisitor workflowVisitor = new WorkflowVisitor(graph, configuration.getProperty("yaml-output-path"));
        while (topologicalOrderIterator.hasNext()) {
            Operator opt = topologicalOrderIterator.next();
            opt.acceptVisitor(workflowVisitor);
        }
        List<Stage> stages = workflowVisitor.getStages(); // 划分好的Stage
//        StageEdgeListener listener = new StageEdgeListener(stages);
//        graph.addGraphListener(listener);
        wrapStageWithHeadTail(stages); // 为每个Stage添加一个对应平台的SourceOpt 和 SinkOpt
//        graph.removeGraphListener(listener);
        Workflow argoWorkflow = new Workflow(new ArgoAdapter(), stages);
        argoWorkflow.execute(); // 将workflow生成为YAML
    }

    /**
     * 不同的Stage会放到不同平台上处理，每个平台上的stage都需要独立的source和sink，
     * 因为需要为拆分后的每个stage都添加一个SourceOperator作为头节点，SinkOperator作为尾节点
     *
     * @param stages 拆分后的所有Stage
     * @throws Exception
     */
    private void wrapStageWithHeadTail(List<Stage> stages) throws Exception {
        String filePath = null;
        for (int i = 0; i < stages.size(); i++) {
            Stage stage = stages.get(i);
            if (i == 0) { // 现在默认所有Stage是线性的 而且 第一个Stage是入口， 所以可以直接通过 i 判断加Source or Sink
                filePath = configuration.getProperty("yaml-output-path")
                        + String.format("stage-%s-output@%s", stage.getId(), Util.generateID());
                checkAndAddSink(stage, filePath);
            } else if (i == stages.size() - 1) {
                checkAndAddSource(stage, filePath);
            } else {
                checkAndAddSource(stage, filePath);
                filePath = configuration.getProperty("yaml-output-path")
                        + String.format("stage-%s-output@%s", stage.getId(), Util.generateID());
                checkAndAddSink(stage, filePath);
            }
        }
    }

    private void checkAndAddSource(Stage stage, String filePath) {
        // 检查下是不是都是FileSource?
        // heads.stream().allMatch(operator -> operator.getOperatorID().equals("Source"));
        // 先创建一个Source（一般一个Stage内就不会有多个起点了 吧？）
        boolean containSource = stage.getHeads().stream()
                .anyMatch(operator -> operator.getOperatorID().contains("Source"));
        if (!containSource) { // 如果头节点不包含任何Source类节点时，插入Source todo 选择插入哪种Source
            try {
                Operator sourceOperator = OperatorFactory.createOperator("source");
                sourceOperator.selectEntity(stage.getPlatform());
                sourceOperator.setParamValue("inputPath", filePath);
                List<Operator> heads = stage.getHeads();

                graph.addVertex(sourceOperator);
                stage.getGraph().addVertex(sourceOperator);
                // 拿到Stage的所有头节点; 从大图里找 当前Stage的Head的 所有上一跳， 删除他们之间的边( 会用新的SourceOpt链接二者
                for (Operator headOperator : heads) {
                    List<Operator> predecessors = Graphs.predecessorListOf(graph, headOperator);
                    for (Operator predecessor : predecessors) {
                        graph.removeEdge(predecessor, headOperator);
                    }
                    graph.addEdge(sourceOperator, headOperator);
                    stage.getGraph().addEdge(sourceOperator, headOperator);
                }
            } catch (Exception e) {
                LOGGER.info("这硬编码创建了SourceOperator，更新其他Opt.配置相关的代码可能会使此处无法创建SourceOperator");
                e.printStackTrace();
            }
        }

    }

    private void checkAndAddSink(Stage stage, String filePath) {
        boolean containSink = stage.getTails().stream()
                .anyMatch(operator -> operator.getOperatorID().contains("Sink"));
        if (!containSink) {
            try {
                Operator sinkOperator = OperatorFactory.createOperator("sink");
                sinkOperator.selectEntity(stage.getPlatform());
                sinkOperator.setParamValue("outputPath", filePath);
                List<Operator> tails = stage.getTails();

                graph.addVertex(sinkOperator);
                stage.getGraph().addVertex(sinkOperator);
                // 拿到Stage的所有尾节点; 从大图里找 当前Stage的Tail的 所有下一跳， 删除他们之间的边( 会用新的SinkOpt链接二者
                for (Operator tailOperator : tails) { // 这不能用Stream 或者 forEach 的(传入Consumer的)写法，出于某种原因会跳过Event的发射
                    List<Operator> successors = Graphs.successorListOf(graph, tailOperator);
                    for (Operator successor : successors) {
                        graph.removeEdge(tailOperator, successor);
                    }
                    graph.addEdge(tailOperator, sinkOperator);

                    stage.getGraph().addEdge(tailOperator, sinkOperator);
                }
            } catch (Exception e) {
                LOGGER.info("这硬编码创建了SinkOperator，更新其他Opt.配置相关的代码可能会使此处无法创建Source、Sink Operator");
                e.printStackTrace();
            }
        }
    }

    /**
     * 把PlanBuilder代表的Graph转为Yaml格式的字符串
     */
    public void toYaml(Writer writer) { // 或许放到YamlUtil里更好？
        Map<String, Object> yamlMap = ArgoAdapter.graph2Yaml(graph);
        YamlUtil.writeYaml(writer, yamlMap);
    }

    /**
     * 设置平台的udf的路径
     *
     * @param platform 平台名称
     * @param udfPath  路径
     */
    public void setPlatformUdfPath(String platform, String udfPath) {
        PlatformFactory.setPlatformArgValue(platform, "--udfPath", udfPath);
    }

}
