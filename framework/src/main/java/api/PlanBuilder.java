
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
import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.swing.mxGraphComponent;
import fdu.daslab.backend.executor.model.Workflow;
import org.javatuples.Pair;
import org.jgrapht.Graphs;
import org.jgrapht.event.GraphEdgeChangeEvent;
import org.jgrapht.event.GraphListener;
import org.jgrapht.event.GraphVertexChangeEvent;
import org.jgrapht.event.VertexSetListener;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultListenableGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class PlanBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanBuilder.class);
    private LinkedList<Operator> pipeline;
    private DataQuanta headDataQuanta = null; // 其实可以有多个head
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
        pipeline = new LinkedList<>();
        // executionPlan = new LinkedList<>();

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

    public boolean addEdges(DataQuanta sourceDataQuanta, DataQuanta targetDataQuanta, List<Pair<String, String>> keyPairs) {
        Channel channel = new Channel(keyPairs);
        return graph.addEdge(sourceDataQuanta.getOperator(), targetDataQuanta.getOperator(), channel);
    }

    public boolean addEdge(DataQuanta sourceDataQuanta, DataQuanta targetDataQuanta, Pair<String, String> keyPair) {
        List<Pair<String, String>> keyPairs = new ArrayList<>();
        if (keyPair == null){
            keyPair = new Pair<>("result", "data");
        }
        keyPairs.add(keyPair);
        return this.addEdges(sourceDataQuanta, targetDataQuanta, keyPairs);
    }

    public DataQuanta readDataFrom(Map<String, String> params) throws Exception {
        DataQuanta dataQuanta = DataQuanta.createInstance("source", params);
        this.headDataQuanta = dataQuanta;
        return this.headDataQuanta; // 不需要connectTo
    }

    public DataQuanta getHeadDataQuanta() {
        return headDataQuanta;
    }

    public void setHeadDataQuanta(DataQuanta headDataQuanta) {
        this.headDataQuanta = headDataQuanta;
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

        LOGGER.info("===========【Stage 3】Visualization ===========");
        this.visualizePlan();
        LOGGER.info("   ");

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
        Operator start = graph.vertexSet()
                .stream()
                .filter(operator -> graph.inDegreeOf(operator)==0)
                .findAny()
                .get();
        BreadthFirstIterator<Operator, Channel> breadthFirstIterator = new BreadthFirstIterator<>(graph, start);
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor();
        while (breadthFirstIterator.hasNext()) {
            breadthFirstIterator.next().acceptVisitor(executionGenerationVisitor);
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
        StageEdgeListener listener = new StageEdgeListener(stages);
        graph.addGraphListener(listener);
        wrapStageWithHeadTail(stages); // 为每个Stage添加一个对应平台的SourceOpt 和 SinkOpt
        graph.removeGraphListener(listener);
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
            if (i == 0) { // 现在默认所有Stage是线性的 而且 第一个Stage是入口
                filePath = configuration.getProperty("yaml-output-path")
                        + String.format("stage-%s-output@%s", stage.getId(), Util.IDSupplier.get());
                checkAndAddSink(stages.get(i), filePath);
            } else if (i == stages.size() - 1) {
                checkAndAddSource(stage, filePath);
            } else {
                checkAndAddSource(stage, filePath);
                filePath = configuration.getProperty("yaml-output-path")
                        + String.format("stage-%s-output@%s", stage.getId(), Util.IDSupplier.get());
                checkAndAddSink(stage, filePath);
            }
        }
    }

    private void checkAndAddSource(Stage stage, String filePath) {
        // 检查下是不是都是FileSource?
        // heads.stream().allMatch(operator -> operator.getOperatorID().equals("Source"));
        // 先创建一个Source（一般一个Stage内就不会有多个起点了 吧？）
        try {
            Operator sourceOperator = OperatorFactory.createOperator("source");
            sourceOperator.selectEntity(stage.getPlatform());
            sourceOperator.setParamValue("inputPath", filePath);
            AsSubgraph<Operator, Channel> subGraph = stage.getGraph();

            graph.addVertex(sourceOperator);
            List<Operator> headOperators = subGraph.vertexSet().stream()
                    .filter(operator -> !Graphs.vertexHasPredecessors(subGraph, operator)).collect(Collectors.toList());
            // 拿到Stage的所有头节点; 从大图里找 当前Stage的Head的 所有上一跳， 删除他们之间的边( 会用新的SourceOpt链接二者
            for (Operator headOperator : headOperators) {
                List<Operator> predecessors = Graphs.predecessorListOf(graph, headOperator);
                for (Operator predecessor : predecessors){
                    graph.removeEdge(predecessor, headOperator);
                }
                graph.addEdge(sourceOperator, headOperator);
            }
        } catch (Exception e) {
            LOGGER.info("这硬编码创建了Source、Sink Operator，更新其他代码可能会使此处无法创建Source、Sink Operator");
            e.printStackTrace();
        }
    }

    private void checkAndAddSink(Stage stage, String filePath) throws Exception {
        // 先创建一个Sink
        Operator sinkOperator = OperatorFactory.createOperator("sink");
        sinkOperator.selectEntity(stage.getPlatform());
        sinkOperator.setParamValue("outputPath", filePath);

        AsSubgraph<Operator, Channel> subGraph = stage.getGraph();

        graph.addVertex(sinkOperator);
        List<Operator> tailOperators = subGraph.vertexSet()
                .stream()
                .filter(operator -> !Graphs.vertexHasSuccessors(subGraph, operator)).collect(Collectors.toList());
        // 拿到Stage的所有尾节点; 从大图里找 当前Stage的Tail的 所有下一跳， 删除他们之间的边( 会用新的SinkOpt链接二者
        for (Operator tailOperator : tailOperators) { // 这不能用Stream 或者 forEach 的(传入Consumer的)写法，出于某种原因会跳过Event的发射
            List<Operator> successors = Graphs.successorListOf(graph, tailOperator);
            for (Operator successor : successors){
                graph.removeEdge(tailOperator, successor);
            }
            // successors.forEach(targetOpt -> graph.removeEdge(tailOperator, targetOpt));
            graph.addEdge(tailOperator, sinkOperator);
        }

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

    class StageEdgeListener implements GraphListener<Operator, Channel> {
        private List<Stage> stages;

        public StageEdgeListener(List<Stage> stages) {
            this.stages = stages;
        }

        @Override
        public void edgeAdded(GraphEdgeChangeEvent<Operator, Channel> graphEdgeChangeEvent) {
            Operator sourceOpt = graphEdgeChangeEvent.getEdgeSource();
            Operator targetOpt = graphEdgeChangeEvent.getEdgeTarget();
            Channel theEdge = graphEdgeChangeEvent.getEdge();

            if(sourceOpt.getOperatorID().contains("Source")){
                // 添加Source的时候，其Opposite一定在某个Stage里
                Stage stage = stages.stream().filter(stage1 -> stage1.getGraph().containsVertex(targetOpt)).findAny().get();
                stage.getGraph().addVertex(sourceOpt);
                stage.getGraph().addEdge(sourceOpt, targetOpt, theEdge);
            }else if(targetOpt.getOperatorID().contains("Sink")){
                Stage stage = stages.stream().filter(stage1 -> stage1.getGraph().containsVertex(sourceOpt)).findAny().get();
                stage.getGraph().addVertex(targetOpt);
                stage.getGraph().addEdge(sourceOpt, targetOpt, theEdge);
            }
        }

        @Override
        public void edgeRemoved(GraphEdgeChangeEvent<Operator, Channel> graphEdgeChangeEvent) {
        }

        @Override
        public void vertexAdded(GraphVertexChangeEvent<Operator> graphVertexChangeEvent) {
        }

        @Override
        public void vertexRemoved(GraphVertexChangeEvent<Operator> graphVertexChangeEvent) {
        }
    }

}
