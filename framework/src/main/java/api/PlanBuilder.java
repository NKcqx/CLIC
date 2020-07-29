
package api;

import adapters.ArgoAdapter;
import basic.Configuration;
import basic.Stage;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.platforms.PlatformFactory;
import basic.traversal.AbstractTraversal;
import basic.traversal.BfsTraversal;
import basic.traversal.TopTraversal;
import basic.visitors.ExecutionGenerationVisitor;
import basic.visitors.PrintVisitor;
import basic.visitors.WorkflowVisitor;
import channel.Channel;
import fdu.daslab.backend.executor.model.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class PlanBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanBuilder.class);
    private LinkedList<Operator> pipeline;
    private DataQuanta headDataQuanta = null; // 其实可以有多个head
    // 现在最简单粗暴的方法是将图存储在PlanBuilder中
    private List<DataQuanta> dataQuantaList = new ArrayList<>();
    private DataQuanta presentDataQuanta = null; // head永远是present的上一个节点

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
    }

    public PlanBuilder() throws ParserConfigurationException, SAXException, IOException {
        this(new Configuration());
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

    /**
     * 把 headOperator 交给各类Visitor
     * 1: Optimize the pipeline structure
     * 2: Mapping operator to Executable, Platform depended Operator
     * 3. Run
     */
    public void execute() throws Exception {
        this.logging("===========【Stage 1】Get User Defined Plan ===========");
        this.printPlan();
        this.logging("   ");

        this.logging("===========【Stage 2】Choose best Operator implementation ===========");
        this.optimizePlan();
        this.printPlan();
        this.logging("   ");

        this.logging("===========【Stage 3】execute plan ==========");
        this.executePlan();

    }

    public void printPlan() {
        this.logging("Current Plan:");
        AbstractTraversal planTraversal = new TopTraversal(this.getHeadDataQuanta().getOperator());
        PrintVisitor printVisitor = new PrintVisitor(planTraversal);
        printVisitor.startVisit();
    }

    public void optimizePlan() {
        AbstractTraversal planTraversal = new BfsTraversal(this.getHeadDataQuanta().getOperator());
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor(planTraversal);
        executionGenerationVisitor.startVisit();
    }

    private void executePlan() throws Exception {
//        AbstractTraversal planTraversal = new BfsTraversal(this.getHeadDataQuanta().getOperator());
//        ExecuteVisitor executeVisitor = new ExecuteVisitor(planTraversal);
//        executeVisitor.startVisit();
        TopTraversal planTraversal = new TopTraversal(this.getHeadDataQuanta().getOperator());
        // PipelineVisitor executeVisitor = new PipelineVisitor(planTraversal);
        // executeVisitor.startVisit();
        // 获取所有的operator
        // List<Operator> allOperators = executeVisitor.getAllOperators();
        // 调用argo平台运行
        // Pipeline argoPipeline = new Pipeline(new ArgoAdapter(), allOperators);
        // argoPipeline.execute();

        /**
         * 1. 调用WorkflowVisitor 得到Stages
         * 2. 创建Workflow 传入stages和ArgoAdapter（adapter使用新写的setArgoNode 接收List of Stage）
         * 3. Workflow,execute()
         */
        WorkflowVisitor workflowVisitor = new WorkflowVisitor(planTraversal);
        workflowVisitor.startVisit();
        List<Stage> stages = workflowVisitor.getStages(); // 划分好的Stage
        stages = wrapStageWithHeadTail(stages);
        Workflow argoWorkflow = new Workflow(new ArgoAdapter(), stages);
        argoWorkflow.execute(); // 将workflow生成为YAML
//        for (Operator opt : this.pipeline){
//            opt.acceptVisitor(executeVisitor);
//        }
    }

    private Stage insertSink(Stage stage, String filePath) throws Exception {
        // 先创建一个Sink
        Operator sinkOperator = OperatorFactory.createOperator("sink");
        sinkOperator.selectEntity(stage.getPlatform());
        sinkOperator.setParamValue("output", filePath);
        // 再链接两个opt
        Channel channel = new Channel(stage.getTail(), sinkOperator);
        stage.getTail().connectTo(channel);
        sinkOperator.connectFrom(channel);
        // 最后更新stage的首尾
        stage.setTail(sinkOperator);
        return stage;
    }

    private Stage insertSource(Stage stage, String filePath) throws Exception {
        // 先创建一个Source
        Operator sourceOperator = OperatorFactory.createOperator("source");
        sourceOperator.selectEntity(stage.getPlatform());
        sourceOperator.setParamValue("output", filePath);
        // 再链接两个opt
        Channel channel = new Channel(sourceOperator, stage.getHead());
        sourceOperator.connectTo(channel);
        stage.getHead().connectFrom(channel);
        // 最后更新stage的首尾
        stage.setHead(sourceOperator);
        return stage;
    }

    private List<Stage> wrapStageWithHeadTail(List<Stage> stages) throws Exception {
        String code = String.valueOf(new Date().hashCode());
        String filePath = null;
        for (int i=0;i<stages.size();++i){
            Stage stage = stages.get(i);
            Operator stageHead = stage.getHead();
            Operator stageTail = stage.getTail();
            if (i==0){
                // sink file path
                filePath = String.format("stage-%s-output@%s", stage.getId(), code);
                insertSink(stage, filePath);
            }else if(i == stages.size()-1){
                insertSource(stage, filePath);
            }else{
                insertSource(stage, filePath);
                filePath = String.format("stage-%s-output@%s", stage.getId(), code);
                insertSink(stage, filePath);
            }


        }
        return null;
    }

    private LinkedList<Operator> optimizePipeline() {
        this.switchOperator(1, 2);
        return this.pipeline;
    }

    /**
     * 交换pipeline中任意两opt的位置，用于算子重组
     *
     * @param idx1 第一个opt的idx
     * @param idx2 第二个opt的idx
     */
    private void switchOperator(int idx1, int idx2) {
        this.logging(String.format("->    Switching Opt %s @%d with %s @%d",
                this.pipeline.get(idx1).getOperatorID(), idx1,
                this.pipeline.get(idx2).getOperatorID(), idx2
        ));
        assert idx1 < this.pipeline.size() : "idx1是无效的索引";
        assert idx2 < this.pipeline.size() : "idx2是无效的索引";
        Operator opt1 = this.pipeline.get(idx1);
        Operator opt2 = this.pipeline.get(idx2);

        this.pipeline.add(idx1, opt2);
        this.pipeline.remove(idx1 + 1); // 删除原来的opt1

        this.pipeline.add(idx2, opt1);
        this.pipeline.remove(idx2 + 1);
    }

    private void logging(String s) {
        LOGGER.info(s);
    }

    /**
     * 设置平台的udf的路径
     *
     * @param platform 平台名称
     * @param udfPath 路径
     */
    public void setPlatformUdfPath(String platform, String udfPath) {
        PlatformFactory.setPlatformArgValue(platform, "--udfPath", udfPath);
    }
}
